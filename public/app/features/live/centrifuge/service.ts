import Centrifuge from 'centrifuge/dist/centrifuge';
import { LiveDataStreamOptions } from '@grafana/runtime';
import { BehaviorSubject, filter, Observable } from 'rxjs';
import {
  DataQueryResponse,
  LiveChannelAddress,
  LiveChannelConfig,
  LiveChannelConnectionState,
  LiveChannelEvent,
  LiveChannelId,
  LiveChannelPresenceStatus,
  toLiveChannelId,
} from '@grafana/data';
import { CentrifugeLiveChannel } from './channel';
import { LiveDataStream } from './LiveDataStream';

export type CentrifugeSrvDeps = {
  appUrl: string;
  orgId: number;
  orgRole: string;
  sessionId: string;
  liveEnabled: boolean;
  dataStreamSubscriberReadiness: Observable<boolean>;
};

export interface CentrifugeSrv {
  /**
   * Listen for changes to the connection state
   */
  getConnectionState(): Observable<boolean>;

  /**
   * Watch for messages in a channel
   */
  getStream<T>(address: LiveChannelAddress, config: LiveChannelConfig): Observable<LiveChannelEvent<T>>;

  /**
   * Connect to a channel and return results as DataFrames
   */
  getDataStream(options: LiveDataStreamOptions, config: LiveChannelConfig): Observable<DataQueryResponse>;

  /**
   * For channels that support presence, this will request the current state from the server.
   *
   * Join and leave messages will be sent to the open stream
   */
  getPresence(address: LiveChannelAddress, config: LiveChannelConfig): Promise<LiveChannelPresenceStatus>;
}

export type DataStreamSubscriptionKey = string;

export class CentrifugeService implements CentrifugeSrv {
  readonly open = new Map<string, CentrifugeLiveChannel>();
  private readonly liveDataStreamByChannelId: Record<LiveChannelId, LiveDataStream> = {};
  readonly centrifuge: Centrifuge;
  readonly connectionState: BehaviorSubject<boolean>;
  readonly connectionBlocker: Promise<void>;
  private dataStreamSubscriberReady = true;

  constructor(private deps: CentrifugeSrvDeps) {
    deps.dataStreamSubscriberReadiness.subscribe((next) => (this.dataStreamSubscriberReady = next));
    const liveUrl = `${deps.appUrl.replace(/^http/, 'ws')}/api/live/ws`;
    this.centrifuge = new Centrifuge(liveUrl, {});
    this.centrifuge.setConnectData({
      sessionId: deps.sessionId,
      orgId: deps.orgId,
    });
    // orgRole is set when logged in *or* anonomus users can use grafana
    if (deps.liveEnabled && deps.orgRole !== '') {
      this.centrifuge.connect(); // do connection
    }
    this.connectionState = new BehaviorSubject<boolean>(this.centrifuge.isConnected());
    this.connectionBlocker = new Promise<void>((resolve) => {
      if (this.centrifuge.isConnected()) {
        return resolve();
      }
      const connectListener = () => {
        resolve();
        this.centrifuge.removeListener('connect', connectListener);
      };
      this.centrifuge.addListener('connect', connectListener);
    });

    // Register global listeners
    this.centrifuge.on('connect', this.onConnect);
    this.centrifuge.on('disconnect', this.onDisconnect);
    this.centrifuge.on('publish', this.onServerSideMessage);
  }

  //----------------------------------------------------------
  // Internal functions
  //----------------------------------------------------------

  private onConnect = (context: any) => {
    this.connectionState.next(true);
  };

  private onDisconnect = (context: any) => {
    this.connectionState.next(false);
  };

  private onServerSideMessage = (context: any) => {
    console.log('Publication from server-side channel', context);
  };

  /**
   * Get a channel.  If the scope, namespace, or path is invalid, a shutdown
   * channel will be returned with an error state indicated in its status
   */
  private getChannel<TMessage>(addr: LiveChannelAddress, config: LiveChannelConfig): CentrifugeLiveChannel<TMessage> {
    const id = `${this.deps.orgId}/${addr.scope}/${addr.namespace}/${addr.path}`;
    let channel = this.open.get(id);
    if (channel != null) {
      return channel;
    }

    channel = new CentrifugeLiveChannel(id, addr);
    channel.shutdownCallback = () => {
      this.open.delete(id); // remove it from the list of open channels
    };
    this.open.set(id, channel);

    // Initialize the channel in the background
    this.initChannel(config, channel).catch((err) => {
      if (channel) {
        channel.currentStatus.state = LiveChannelConnectionState.Invalid;
        channel.shutdownWithError(err);
      }
      this.open.delete(id);
    });

    // return the not-yet initalized channel
    return channel;
  }

  private async initChannel(config: LiveChannelConfig, channel: CentrifugeLiveChannel): Promise<void> {
    const events = channel.initalize(config);
    if (!this.centrifuge.isConnected()) {
      await this.connectionBlocker;
    }
    channel.subscription = this.centrifuge.subscribe(channel.id, events);
    return;
  }

  //----------------------------------------------------------
  // Exported functions
  //----------------------------------------------------------

  /**
   * Listen for changes to the connection state
   */
  getConnectionState() {
    return this.connectionState.asObservable();
  }

  /**
   * Watch for messages in a channel
   */
  getStream<T>(address: LiveChannelAddress, config: LiveChannelConfig): Observable<LiveChannelEvent<T>> {
    return this.getChannel<T>(address, config).getStream();
  }

  private createSubscriptionKey = (options: LiveDataStreamOptions): DataStreamSubscriptionKey =>
    options.key ?? `xstr/${streamCounter++}`;

  private getLiveDataStream = (options: LiveDataStreamOptions, config: LiveChannelConfig): LiveDataStream => {
    const channelId = toLiveChannelId(options.addr);
    const existingStream = this.liveDataStreamByChannelId[channelId];

    if (existingStream) {
      return existingStream;
    }

    const channel = this.getChannel(options.addr, config);
    this.liveDataStreamByChannelId[channelId] = new LiveDataStream({
      channelId,
      config,
      onShutdown: () => {
        delete this.liveDataStreamByChannelId[channelId];
      },
      liveEventsObservable: channel.getStream(),
    });
    return this.liveDataStreamByChannelId[channelId];
  };
  /**
   * Connect to a channel and return results as DataFrames
   */
  getDataStream(options: LiveDataStreamOptions, config: LiveChannelConfig): Observable<DataQueryResponse> {
    const subscriptionKey = this.createSubscriptionKey(options);

    const stream = this.getLiveDataStream(options, config);
    return stream.get(options, subscriptionKey).pipe(filter(() => this.dataStreamSubscriberReady));
  }

  /**
   * For channels that support presence, this will request the current state from the server.
   *
   * Join and leave messages will be sent to the open stream
   */
  getPresence(address: LiveChannelAddress, config: LiveChannelConfig): Promise<LiveChannelPresenceStatus> {
    return this.getChannel(address, config).getPresence();
  }
}

// This is used to give a unique key for each stream.  The actual value does not matter
let streamCounter = 0;
