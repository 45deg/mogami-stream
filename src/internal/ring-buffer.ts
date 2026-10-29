/** Bounded history that grows with input instead of allocating capacity upfront. */
export class RingBuffer<T> {
  private values: T[] = [];
  private head = 0;

  constructor(private readonly capacity: number) {}

  get size(): number {
    return this.values.length;
  }

  push(value: T): void {
    if (this.values.length < this.capacity) this.values.push(value);
    else {
      this.values[this.head] = value;
      this.head = (this.head + 1) % this.capacity;
    }
  }

  get(index: number): T {
    return this.values[(this.head + index) % this.values.length]!;
  }

  snapshot(): T[] {
    if (this.head === 0) return this.values.slice();
    const result = this.values.slice(this.head);
    for (let i = 0; i < this.head; i++) result.push(this.values[i]!);
    return result;
  }

  clear(): void {
    this.values = [];
    this.head = 0;
  }
}
