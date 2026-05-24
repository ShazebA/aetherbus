// Typed wrappers around the FastAPI surface (see backend/app.py).
import { useQuery, useMutation } from "@tanstack/react-query";

export interface StreamSummary {
  name: string;
  subjects: string[];
}

export interface EventSummary {
  id: string;
  source: string;
  type: string;
  time: string;
  has_dataref: boolean;
}

export interface EventDetail {
  envelope: Record<string, unknown>;
  inline_data: unknown | null;
  dataref: string | null;
}

async function fetchJson<T>(path: string): Promise<T> {
  const resp = await fetch(path);
  if (!resp.ok) {
    throw new Error(`${path} → ${resp.status}`);
  }
  return resp.json();
}

export function useStreams() {
  return useQuery<StreamSummary[]>({
    queryKey: ["streams"],
    queryFn: () => fetchJson("/api/streams"),
  });
}

export function useEvents(streamName: string | null, topic: string, limit = 50) {
  return useQuery<EventSummary[]>({
    queryKey: ["events", streamName, topic, limit],
    queryFn: () =>
      fetchJson(
        `/api/streams/${encodeURIComponent(streamName!)}/events?topic=${encodeURIComponent(topic)}&limit=${limit}`,
      ),
    enabled: Boolean(streamName) && Boolean(topic),
  });
}

export function useEventDetail(eventId: string | null, topic: string) {
  return useQuery<EventDetail>({
    queryKey: ["event", eventId, topic],
    queryFn: () =>
      fetchJson(
        `/api/events/${encodeURIComponent(eventId!)}?topic=${encodeURIComponent(topic)}`,
      ),
    enabled: Boolean(eventId) && Boolean(topic),
  });
}

export interface ReplayResult {
  count: number;
  event_ids: string[];
  sandbox_url: string | null;
  dry_run: boolean;
}

export function useReplay() {
  return useMutation<
    ReplayResult,
    Error,
    {
      topic: string;
      since?: string;
      until?: string;
      sandbox_url?: string;
      sandbox_transport_type?: string;
    }
  >({
    mutationFn: async (body) => {
      const resp = await fetch("/api/replay", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!resp.ok) {
        const detail = await resp.text();
        throw new Error(`replay → ${resp.status}: ${detail}`);
      }
      return (await resp.json()) as ReplayResult;
    },
  });
}
