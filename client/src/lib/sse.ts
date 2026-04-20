import { useEffect, useRef, useState } from "react";
import type { TickData } from "@shared/schema";

const API_BASE = "__PORT_5000__".startsWith("__") ? "" : "__PORT_5000__";

export function useSSE() {
  const [data, setData] = useState<TickData | null>(null);
  const [connected, setConnected] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    let cancelled = false;
    const connect = () => {
      if (cancelled) return;
      const es = new EventSource(`${API_BASE}/api/stream`);
      eventSourceRef.current = es;

      es.onopen = () => setConnected(true);
      es.onmessage = (event) => {
        try {
          const parsed = JSON.parse(event.data);
          setData(parsed);
          setConnected(true);
        } catch (e) {
          console.error("SSE parse error", e);
        }
      };
      es.onerror = () => {
        setConnected(false);
        es.close();
        if (!cancelled) setTimeout(connect, 3000);
      };
    };
    connect();
    return () => {
      cancelled = true;
      eventSourceRef.current?.close();
    };
  }, []);

  return { data, connected };
}

export async function postPause(paused: boolean): Promise<void> {
  await fetch(`${API_BASE}/api/toggle-trading`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ paused }),
  });
}
