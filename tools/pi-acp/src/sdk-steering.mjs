export class SteeringDeliveryGate {
  #queue = [];

  get size() {
    return this.#queue.filter((entry) => entry.command).length;
  }

  enqueue(command, message) {
    const entry = { command, message };
    this.#queue.push(entry);
    return entry;
  }

  enqueueInternal(message) {
    const entry = { command: null, message };
    this.#queue.push(entry);
    return entry;
  }

  remove(entry) {
    const index = this.#queue.indexOf(entry);
    if (index < 0) return false;
    this.#queue.splice(index, 1);
    return true;
  }

  observeQueue(messages, accept, reject) {
    const remaining = Array.isArray(messages) ? messages : [];
    const removedCount = this.#queue.length - remaining.length;
    const matches =
      removedCount >= 0 &&
      this.#queue
        .slice(removedCount)
        .every((entry, index) => entry.message === remaining[index]);
    if (!matches) {
      this.rejectAll(reject);
      this.#queue = remaining.map((message) => ({ command: null, message }));
      return;
    }
    for (const entry of this.#queue.splice(0, removedCount)) {
      if (entry.command) accept(entry.command);
    }
  }

  rejectAll(reject) {
    for (const entry of this.#queue) {
      if (!entry.command) continue;
      reject(entry.command);
      entry.command = null;
    }
  }
}
