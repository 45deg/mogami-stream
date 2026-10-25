/** Occupied slots are written once and cleared before reuse. */
class ArraySlots<T> {
  private values: (T | undefined)[] = [];
  size = 0;

  get(index: number): T | undefined {
    return this.values[index];
  }

  set(index: number, value: T): void {
    this.values[index] = value;
    this.size++;
  }

  delete(index: number): void {
    this.values[index] = undefined;
    this.size--;
  }

  clear(): void {
    this.values = [];
    this.size = 0;
  }
}

export function slotQueue<T>(capacity: number): Map<number, T> | ArraySlots<T> {
  // Limit array slots to measured small queues; large capacities remain sparse.
  return capacity <= 256 ? new ArraySlots<T>() : new Map<number, T>();
}
