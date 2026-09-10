import { useCallback, useEffect, useRef, useState } from "react";

interface PollingState<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  refresh: () => void;
}

/** Polls `fetcher` every `intervalMs` and on mount. Ignores stale in-flight responses. */
export function usePolling<T>(fetcher: () => Promise<T>, intervalMs = 7000): PollingState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const requestId = useRef(0);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const run = useCallback(() => {
    const id = ++requestId.current;
    fetcherRef.current()
      .then((result) => {
        if (id === requestId.current) {
          setData(result);
          setError(null);
        }
      })
      .catch((err: Error) => {
        if (id === requestId.current) setError(err.message);
      })
      .finally(() => {
        if (id === requestId.current) setLoading(false);
      });
  }, []);

  useEffect(() => {
    run();
    const handle = setInterval(run, intervalMs);
    return () => clearInterval(handle);
  }, [run, intervalMs]);

  return { data, error, loading, refresh: run };
}
