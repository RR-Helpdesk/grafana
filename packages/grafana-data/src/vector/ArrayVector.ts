import { MutableVector, Vector } from '../types/vector';
import { FunctionalVector } from './FunctionalVector';

/**
 * @public
 */
export class ArrayVector<T = any> extends FunctionalVector<T> implements MutableVector<T> {
  buffer: T[];

  constructor(buffer?: T[]) {
    super();
    this.buffer = buffer ? buffer : [];
  }

  get length() {
    return this.buffer.length;
  }

  add(value: T) {
    this.buffer.push(value);
  }

  concat(values: T[] | Vector<T>) {
    // @ts-ignore
    // TODO remove
    this.buffer = this.buffer.concat(values);
    return this;
  }

  get(index: number): T {
    return this.buffer[index];
  }

  set(index: number, value: T) {
    this.buffer[index] = value;
  }

  reverse() {
    this.buffer.reverse();
  }

  toArray(): T[] {
    return this.buffer;
  }

  toJSON(): T[] {
    return this.buffer;
  }
}
