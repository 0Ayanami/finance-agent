import type { RuntimeEvent } from "@/types/runtime";

export const API_PREFIX = "/api/agent";

export async function agentApi<T>(path: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(`${API_PREFIX}${path}`, {
    ...options,
    cache: "no-store",
    headers: { "Content-Type": "application/json", "X-Agent-Client": "web", ...options.headers },
  });
  if (!response.ok) {
    const detail: unknown = await response.json().catch(() => null);
    const message = detail && typeof detail === "object" && "detail" in detail && typeof detail.detail === "string"
      ? detail.detail : `HTTP ${response.status}: ${response.statusText}`;
    throw new Error(message);
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

type HistoryRequest = { before: number } | { part: "user" } | { part: "remaining"; through: number };

export function historyPage(sessionId: string, signal: AbortSignal, request?: HistoryRequest) {
  const query = request ? `?${new URLSearchParams(Object.entries(request).map(([key, value]) => [key, String(value)]))}` : "";
  return agentApi<{events: RuntimeEvent[]; cursor: number; before: number | null}>(
    `/sessions/${sessionId}/history${query}`, { signal },
  );
}
