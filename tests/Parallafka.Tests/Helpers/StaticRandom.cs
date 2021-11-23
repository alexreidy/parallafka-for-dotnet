using System;

namespace Parallafka.Tests
{
    public static class StaticRandom
    {
        private static readonly Random Rnd = new();

        public static void Use(Action<Random> use)
        {
            lock (Rnd)
            {
                use(Rnd);
            }
        }

        public static T Use<T>(Func<Random, T> use)
        {
            lock (Rnd)
            {
                return use(Rnd);
            }
        }

    }
}