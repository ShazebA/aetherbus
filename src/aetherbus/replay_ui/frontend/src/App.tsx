import { useEffect, useMemo, useRef, useState } from "react";
import { Activity, Radio, Search, X } from "lucide-react";
import clsx from "clsx";
import { twMerge } from "tailwind-merge";

import {
  type EventSummary,
  useEventDetail,
  useEvents,
  useReplayDryRun,
  useStreams,
} from "./api";

// ---------------------------------------------------------------------------
// tiny `cn` helper — same pattern shadcn uses
const cn = (...classes: (string | false | null | undefined)[]) =>
  twMerge(clsx(classes));

// ---------------------------------------------------------------------------
// Inline UI primitives (no shadcn CLI; we just style what we need)

function Button({
  variant = "primary",
  className,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "secondary" | "ghost";
}) {
  return (
    <button
      {...props}
      className={cn(
        "inline-flex items-center justify-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium",
        "transition-colors disabled:cursor-not-allowed disabled:opacity-50",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/40",
        variant === "primary" &&
          "bg-accent text-bg hover:bg-accent/90",
        variant === "secondary" &&
          "border border-border bg-transparent text-fg hover:bg-surface",
        variant === "ghost" &&
          "bg-transparent text-fg hover:bg-surface",
        className,
      )}
    />
  );
}

function Badge({ children, className }: React.PropsWithChildren<{ className?: string }>) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md px-1.5 py-0.5 text-[10px] font-medium",
        "bg-accent/15 text-accent",
        className,
      )}
    >
      {children}
    </span>
  );
}

function Input(props: React.InputHTMLAttributes<HTMLInputElement>) {
  return (
    <input
      {...props}
      className={cn(
        "flex h-9 w-full rounded-md border border-border bg-bg px-3 py-1 text-sm",
        "placeholder:text-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/40",
        props.className,
      )}
    />
  );
}

// ---------------------------------------------------------------------------
// WebSocket live-tail hook

function useLiveTail(
  streamName: string | null,
  topic: string,
  onEvent: (e: EventSummary) => void,
  enabled: boolean,
) {
  const callbackRef = useRef(onEvent);
  callbackRef.current = onEvent;
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    if (!enabled || !streamName || !topic) return;
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${proto}//${location.host}/api/streams/${encodeURIComponent(streamName)}/tail?topic=${encodeURIComponent(topic)}`;
    const ws = new WebSocket(url);

    ws.onopen = () => setConnected(true);
    ws.onmessage = (msg) => {
      try {
        callbackRef.current(JSON.parse(msg.data) as EventSummary);
      } catch (e) {
        console.error("tail: bad payload", e);
      }
    };
    ws.onclose = () => setConnected(false);
    ws.onerror = (e) => console.error("tail error", e);

    return () => {
      ws.close();
    };
  }, [enabled, streamName, topic]);

  return connected;
}

// ---------------------------------------------------------------------------
// Pages

function StreamsSidebar({
  active,
  onPick,
}: {
  active: string | null;
  onPick: (name: string) => void;
}) {
  const { data, isLoading, error } = useStreams();
  return (
    <aside className="border-r border-border bg-surface w-72 flex flex-col">
      <div className="px-4 py-3 border-b border-border">
        <h2 className="text-[11px] tracking-widest uppercase text-muted font-medium">
          Streams
        </h2>
      </div>
      <div className="flex-1 overflow-y-auto p-3 space-y-1">
        {isLoading && <div className="text-muted text-sm px-2 py-1">Loading…</div>}
        {error && (
          <div className="text-red-400 text-sm px-2 py-1">
            Failed to load streams. Is the backend running?
          </div>
        )}
        {data?.map((s) => (
          <button
            key={s.name}
            onClick={() => onPick(s.name)}
            className={cn(
              "w-full text-left rounded-md px-3 py-2 transition-colors",
              active === s.name
                ? "bg-accent/10 border border-accent/30"
                : "hover:bg-bg",
            )}
          >
            <div className="font-semibold">{s.name}</div>
            <div className="text-[11px] text-muted mt-0.5 truncate font-mono">
              {s.subjects.join(", ")}
            </div>
          </button>
        ))}
      </div>
    </aside>
  );
}

function EventDetailDialog({
  eventId,
  topic,
  onClose,
}: {
  eventId: string | null;
  topic: string;
  onClose: () => void;
}) {
  const dialogRef = useRef<HTMLDialogElement>(null);
  const { data, isLoading } = useEventDetail(eventId, topic);

  useEffect(() => {
    if (eventId) dialogRef.current?.showModal();
    else dialogRef.current?.close();
  }, [eventId]);

  return (
    <dialog
      ref={dialogRef}
      onClose={onClose}
      className="rounded-lg border border-border bg-surface text-fg w-[min(900px,90vw)] p-0 backdrop:bg-black/70"
    >
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <h3 className="font-semibold">Event detail</h3>
        <Button variant="ghost" onClick={onClose} className="p-1 h-auto">
          <X className="w-4 h-4" />
        </Button>
      </div>
      <div className="p-4 space-y-3">
        {isLoading && <div className="text-muted">Loading…</div>}
        {data && (
          <>
            <Section title="Envelope">
              <pre className="text-[12px] font-mono overflow-x-auto bg-bg border border-border rounded-md p-3">
                {JSON.stringify(data.envelope, null, 2)}
              </pre>
            </Section>
            {data.dataref ? (
              <Section title="dataref">
                <code className="text-[12px] font-mono break-all">{data.dataref}</code>
              </Section>
            ) : (
              <Section title="Inline data">
                <pre className="text-[12px] font-mono overflow-x-auto bg-bg border border-border rounded-md p-3">
                  {JSON.stringify(data.inline_data, null, 2)}
                </pre>
              </Section>
            )}
          </>
        )}
      </div>
    </dialog>
  );
}

function Section({
  title,
  children,
}: React.PropsWithChildren<{ title: string }>) {
  return (
    <div>
      <div className="text-[11px] uppercase tracking-widest text-muted mb-1.5">
        {title}
      </div>
      {children}
    </div>
  );
}

function EventsTable({
  rows,
  onPick,
}: {
  rows: EventSummary[];
  onPick: (id: string) => void;
}) {
  if (rows.length === 0) {
    return (
      <div className="text-muted italic text-center py-12 border border-dashed border-border rounded-lg">
        No events yet. Publish one, or hit <strong className="not-italic">Load history</strong>.
      </div>
    );
  }
  return (
    <div className="border border-border rounded-lg overflow-hidden">
      <table className="w-full">
        <thead className="bg-surface">
          <tr className="text-left text-[11px] uppercase tracking-widest text-muted">
            <th className="px-3 py-2 font-medium">id</th>
            <th className="px-3 py-2 font-medium">type</th>
            <th className="px-3 py-2 font-medium">source</th>
            <th className="px-3 py-2 font-medium">time</th>
            <th className="px-3 py-2 font-medium"></th>
          </tr>
        </thead>
        <tbody>
          {rows.map((e) => (
            <tr
              key={e.id}
              onClick={() => onPick(e.id)}
              className="border-t border-border hover:bg-surface cursor-pointer transition-colors"
            >
              <td className="px-3 py-2 font-mono text-[12px]">
                {e.id.slice(0, 8)}…
              </td>
              <td className="px-3 py-2 font-mono text-[12px]">{e.type}</td>
              <td className="px-3 py-2 font-mono text-[12px] text-muted">{e.source}</td>
              <td className="px-3 py-2 font-mono text-[12px] text-muted">
                {new Date(e.time).toLocaleTimeString()}
              </td>
              <td className="px-3 py-2">
                {e.has_dataref && <Badge>dataref</Badge>}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Root

export function App() {
  const [activeStream, setActiveStream] = useState<string | null>(null);
  const [topic, setTopic] = useState("dev.aetherbus.research.finding.v1");
  const [tailing, setTailing] = useState(false);
  const [pickedEvent, setPickedEvent] = useState<string | null>(null);
  const [liveEvents, setLiveEvents] = useState<EventSummary[]>([]);
  const [showLive, setShowLive] = useState(false);
  const replayDryRun = useReplayDryRun();

  // When the user clicks "Load history" we display the history results;
  // when tailing is enabled we display the live buffer.
  const history = useEvents(showLive ? null : activeStream, showLive ? "" : topic);

  const connected = useLiveTail(activeStream, topic, (ev) => {
    setLiveEvents((prev) => [ev, ...prev].slice(0, 200));
  }, tailing);

  const rows = useMemo(() => {
    if (tailing || showLive) return liveEvents;
    return history.data ?? [];
  }, [tailing, showLive, liveEvents, history.data]);

  return (
    <div className="flex flex-col h-screen">
      <header className="border-b border-border bg-surface px-4 py-3 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Activity className="w-4 h-4 text-accent" />
          <h1 className="font-semibold tracking-tight">AetherBus Replay</h1>
        </div>
        <div className="flex items-center gap-2 text-xs text-muted">
          <span
            className={cn(
              "inline-block w-2 h-2 rounded-full",
              connected ? "indicator-live" : "bg-muted/40",
            )}
          />
          <span>{connected ? "Live" : tailing ? "Connecting…" : "Idle"}</span>
        </div>
      </header>

      <div className="flex flex-1 overflow-hidden">
        <StreamsSidebar active={activeStream} onPick={setActiveStream} />

        <main className="flex-1 overflow-auto p-6 space-y-4">
          <div className="flex items-center gap-2">
            <Search className="w-4 h-4 text-muted shrink-0" />
            <Input
              placeholder="topic (e.g. dev.aetherbus.research.finding.v1)"
              value={topic}
              onChange={(e) => setTopic(e.target.value)}
            />
            <Button
              onClick={() => {
                setShowLive(false);
                setLiveEvents([]);
                history.refetch();
              }}
              disabled={!activeStream || !topic}
            >
              Load history
            </Button>
            <Button
              variant={tailing ? "secondary" : "primary"}
              onClick={() => {
                setShowLive(!tailing);
                setLiveEvents([]);
                setTailing(!tailing);
              }}
              disabled={!activeStream || !topic}
            >
              <Radio className="w-3.5 h-3.5" />
              {tailing ? "Stop tail" : "Tail"}
            </Button>
            <Button
              variant="secondary"
              onClick={() => replayDryRun.mutate({ topic })}
              disabled={!topic}
              title="Returns the event IDs that would replay (M5c performs the sandbox publish)"
            >
              Replay (dry run)
            </Button>
          </div>

          {replayDryRun.data && (
            <div className="border border-border bg-surface rounded-md p-3 text-sm">
              <span className="text-muted">Would replay </span>
              <span className="font-semibold">{replayDryRun.data.count}</span>
              <span className="text-muted"> events.</span>
            </div>
          )}

          {!activeStream && (
            <div className="text-muted italic text-center py-12 border border-dashed border-border rounded-lg">
              Pick a stream from the sidebar to begin.
            </div>
          )}

          {activeStream && (
            <EventsTable rows={rows} onPick={setPickedEvent} />
          )}
        </main>
      </div>

      <EventDetailDialog
        eventId={pickedEvent}
        topic={topic}
        onClose={() => setPickedEvent(null)}
      />
    </div>
  );
}
