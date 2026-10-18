export async function forSeeds(run) {
  const first = Number(process.env.MOGAMI_FUZZ_SEED ?? 1);
  const runs = Number(process.env.MOGAMI_FUZZ_RUNS ?? 128);
  if (
    !Number.isSafeInteger(first) ||
    first < 1 ||
    !Number.isSafeInteger(runs) ||
    runs < 1 ||
    first + runs - 1 > 0xffff_ffff
  )
    throw new RangeError(
      "Fuzz seeds must stay within 1..4294967295; runs must be positive.",
    );
  for (let seed = first; seed < first + runs; seed++) {
    let state = seed;
    const pick = (limit) => {
      state ^= state << 13;
      state ^= state >>> 17;
      state ^= state << 5;
      return (state >>> 0) % limit;
    };
    try {
      await run(pick);
    } catch (error) {
      throw new Error(
        `Fuzz seed ${seed} failed (MOGAMI_FUZZ_SEED=${seed} MOGAMI_FUZZ_RUNS=1).`,
        { cause: error },
      );
    }
  }
}
