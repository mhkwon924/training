public interface Task<TInput, TOutput> {
  TOutput compute(TInput input);
}
