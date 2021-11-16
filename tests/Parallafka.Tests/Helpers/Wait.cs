using System;
using System.Threading.Tasks;

namespace Parallafka.Tests
{
    public static class Wait
    {
        public static async Task UntilAsync(
            string desiredStateDescription,
            Func<Task<bool>> predicateAsync,
            TimeSpan timeout,
            TimeSpan? retryDelay = null,
            Func<Task> onTimeoutAsync = null,
            Func<string> contextProvider = null)
        {
            var timeoutTask = Task.Delay(timeout);
            while (!timeoutTask.IsCompleted)
            {
                if (await predicateAsync.Invoke())
                {
                    return;
                }
                await Task.Delay(retryDelay ?? TimeSpan.FromMilliseconds(150));
            }

            if (onTimeoutAsync != null)
            {
                await onTimeoutAsync.Invoke();
                return;
            }

            string context = contextProvider?.Invoke() ?? string.Empty;

            throw new Exception($"Timed out waiting for: {desiredStateDescription} ... {context}");
        }

        public static Task UntilAsync(string desiredStateDescription, Func<Task> assertionAsync, TimeSpan timeout, TimeSpan? retryDelay = null)
        {
            return UntilAsync(desiredStateDescription,
                async () =>
                {
                    try
                    {
                        await assertionAsync.Invoke();
                        return true;
                    }
                    catch
                    {
                        return false;
                    }
                },
                timeout,
                retryDelay,
                onTimeoutAsync: async () =>
                {
                    await assertionAsync.Invoke();

                    throw new Exception($"Timed out waiting for: {desiredStateDescription}");
                });
        }

        public static async Task ForTaskOrTimeoutAsync(Task task, TimeSpan timeout, Action onTimeout)
        {
            var timeoutTask = Task.Delay(timeout);
            await Task.WhenAny(task, timeoutTask);
            if (timeoutTask.IsCompleted && !task.IsCompleted)
            {
                onTimeout.Invoke();
            }
        }
    }
}