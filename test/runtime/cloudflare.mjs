import { runContracts } from "./contracts.mjs";
export default {
  async fetch() {
    try {
      return Response.json({ passed: await runContracts() });
    } catch (error) {
      return Response.json(
        { error: String(error?.stack ?? error) },
        { status: 500 },
      );
    }
  },
};
