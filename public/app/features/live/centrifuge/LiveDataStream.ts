import { LiveDataStreamOptions } from '@grafana/runtime';
import { toDataQueryError } from '@grafana/runtime/src/utils/toDataQueryError';
import {
  DataFrameDTO,
  DataFrameJSON,
  dataFrameToJSON,
  DataQueryResponse,
  Field,
  isLiveChannelMessageEvent,
  isLiveChannelStatusEvent,
  LiveChannelConfig,
  LiveChannelConnectionState,
  LiveChannelEvent,
  LiveChannelId,
  LoadingState,
  StreamingDataFrame,
  StreamingFrameAction,
  StreamingFrameOptions,
  toFilteredDataFrameDTO,
} from '@grafana/data';
import { map, Observable, ReplaySubject, Subject, Subscription } from 'rxjs';
import { DataStreamSubscriptionKey } from './service';
import { StreamPacketInfo } from '@grafana/data/src/dataframe/StreamingDataFrame';

const shutdownDelayInMs = 5000;

type DataStreamHandlerDeps<T> = {
  config: LiveChannelConfig;
  channelId: LiveChannelId;
  liveEventsObservable: Observable<LiveChannelEvent<T>>;
  onShutdown: () => void;
};

type NonStreamingResponse = Omit<DataQueryResponse, 'state'> & {
  state: Exclude<DataQueryResponse['state'], LoadingState.Streaming>;
};

type StreamingResponse = Omit<DataQueryResponse, 'state'> & {
  state: LoadingState.Streaming;
  packetInfo: StreamPacketInfo;
};

const isStreamingResponse = (dataQueryResponse: DataQueryResponse): dataQueryResponse is StreamingResponse =>
  dataQueryResponse.state === LoadingState.Streaming;

const defaultStreamingFrameOptions: Readonly<StreamingFrameOptions> = {
  maxLength: 100,
  maxDelta: Infinity,
  action: StreamingFrameAction.Append,
};

// TODO: move to @grafana-data
// make it return a DataFrame chunk type (?)
const lastNValues = (frame: DataFrameDTO, n: number): DataFrameDTO & { isChunk: boolean } => {
  return {
    ...frame,
    fields: frame.fields.map((f) => ({
      ...f,
      values: (f.values as unknown[]).slice(f.values!.length - n),
    })),
    isChunk: true,
  };
};

export class LiveDataStream<T = unknown> {
  private frameBuffer: StreamingDataFrame;
  private liveEventsSubscription: Subscription;
  private dataQueryResponseStream: Subject<StreamingResponse | NonStreamingResponse> = new ReplaySubject(1);

  constructor(private deps: DataStreamHandlerDeps<T>) {
    this.frameBuffer = new StreamingDataFrame(undefined, defaultStreamingFrameOptions);
    this.liveEventsSubscription = deps.liveEventsObservable.subscribe({
      error: this.onError,
      complete: this.onComplete,
      next: this.onNext,
    });
  }

  private shutdown = () => {
    this.liveEventsSubscription.unsubscribe();
    this.deps.onShutdown();
  };

  private onError = (err: any) => {
    console.log('LiveQuery [error]', { err }, this.deps.channelId);
    this.dataQueryResponseStream.next({
      state: LoadingState.Error,
      data: [this.frameBuffer],
      error: toDataQueryError(err),
    });
    this.shutdown();
  };

  private onComplete = () => {
    console.log('LiveQuery [complete]', this.deps.channelId);
    this.dataQueryResponseStream.complete();
    this.shutdown();
  };

  private onNext = (evt: LiveChannelEvent) => {
    if (isLiveChannelMessageEvent(evt)) {
      this.process(evt.message);
      return;
    }

    const liveChannelStatusEvent = isLiveChannelStatusEvent(evt);
    if (liveChannelStatusEvent && evt.error) {
      this.dataQueryResponseStream.next({
        state: LoadingState.Error,
        data: [this.frameBuffer],
        error: {
          ...toDataQueryError(evt.error),
          message: `Streaming channel error: ${evt.error.message}`,
        },
      });
      return;
    }

    if (
      liveChannelStatusEvent &&
      (evt.state === LiveChannelConnectionState.Connected || evt.state === LiveChannelConnectionState.Pending) &&
      evt.message
    ) {
      this.process(evt.message);
    }
  };

  private process = (msg: DataFrameJSON) => {
    const packetInfo = this.frameBuffer.push(msg);

    this.dataQueryResponseStream.next({
      state: LoadingState.Streaming,
      data: [this.frameBuffer],
      packetInfo,
    });
  };

  private resizeBuffer = (options: LiveDataStreamOptions) => {
    const bufferOptions = options.buffer;
    if (bufferOptions && this.frameBuffer.needsResizing(bufferOptions)) {
      this.frameBuffer = this.frameBuffer.resized(bufferOptions);
    }
  };

  private shutdownIfNoSubscribers = () => {
    if (!this.dataQueryResponseStream.observed) {
      this.shutdown();
    }
  };

  get = (options: LiveDataStreamOptions, subKey: DataStreamSubscriptionKey): Observable<DataQueryResponse> => {
    this.resizeBuffer(options);

    if (!this.frameBuffer.hasAtLeastOnePacket() && options.frame) {
      // will skip initial frames from subsequent subscribers
      this.process(dataFrameToJSON(options.frame));
    }

    const fieldsNamesFilter = options.filter?.fields;
    const dataNeedsFiltering = fieldsNamesFilter?.length;
    const fieldPredicate = dataNeedsFiltering ? ({ name }: Field) => fieldsNamesFilter.includes(name) : undefined;

    let fullDataPacketSent = false;
    const transformedSubject = this.dataQueryResponseStream.pipe(
      map((next) => {
        const dataFrame = next.data[0] ? toFilteredDataFrameDTO(next.data[0], fieldPredicate) : undefined;

        const streamingResponse = isStreamingResponse(next);

        if (streamingResponse && dataFrame && !fullDataPacketSent) {
          fullDataPacketSent = true;
          return {
            ...next,
            key: subKey,
            data: [dataFrame],
          };
        }

        if (streamingResponse && dataFrame && fullDataPacketSent) {
          return {
            ...next,
            key: subKey,
            data: [lastNValues(dataFrame, next.packetInfo.length)],
          };
        }

        return {
          ...next,
          data: [dataFrame],
          key: subKey,
        };
      })
    );

    return new Observable<DataQueryResponse>((subscriber) => {
      const sub = transformedSubject.subscribe({
        next: (n) => {
          subscriber.next(n);
        },
        error: (err) => {
          subscriber.error(err);
        },
        complete: () => {
          subscriber.complete();
        },
      });

      return () => {
        // TODO: potentially resize the buffer on unsubscribe
        sub.unsubscribe();

        if (!this.dataQueryResponseStream.observed) {
          setTimeout(this.shutdownIfNoSubscribers, shutdownDelayInMs);
        }
      };
    });
  };
}
