/** An item had no active destination and routing uses `fallback: "error"`. */
export class RouteError extends Error {
  constructor(message = "No active route is available for the item.") {
    super(message);
    this.name = "RouteError";
  }
}
