/**
 * dsh — native DeepSeek Harness provider. Pi is UI only.
 *
 * Every turn executes in a persistent dsh-acp ACP session (same wire
 * martty speaks: newline-delimited JSON-RPC over the agent's stdio).
 * Harness events map onto Pi transcript blocks:
 *   agent text    -> text blocks
 *   agent thought -> thinking blocks
 *   tool calls (+ tails) -> plain text lines (VISIBLE, never executable)
 *
 * Pi's own tools are never declared, called, or executed: the assistant
 * message contains only text/thinking blocks, so Pi's agent loop has
 * nothing to act on. It renders. DSH works.
 */

import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { createProvider, type Provider } from "../models.ts";
import type {
	AssistantMessage,
	AssistantMessageEventStream,
	Model,
	SimpleStreamOptions,
	StreamOptions,
	TextContent,
	ThinkingContent,
	TranscriptContext,
} from "../types.ts";
import { createAssistantMessageEventStream } from "../utils/event-stream.ts";

export const DSH_API = "dsh-acp";
export const DSH_PROVIDER_ID = "dsh";
export const DSH_MODEL_ID = "dsh-harness";
export const DSH_BASE_URL = "dsh://harness";

// ---------------------------------------------------------------------------
// credentials (creds.env + ATLAS mapping, same as the standalone clients)
// ---------------------------------------------------------------------------

function ensureCreds(): void {
	try {
		const raw = readFileSync(join(homedir(), ".config", "atlas", "creds.env"), "utf8");
		for (const line of raw.split("\n")) {
			const t = line.trim();
			if (!t || t.startsWith("#")) continue;
			const eq = t.indexOf("=");
			if (eq < 0) continue;
			const k = t.slice(0, eq).trim();
			if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(k)) continue;
			if (process.env[k] !== undefined) continue;
			process.env[k] = t
				.slice(eq + 1)
				.trim()
				.replace(/^["']|["']$/g, "");
		}
	} catch {
		// no creds file; fall through to whatever env exists
	}
	const pairs: Array<[string, string]> = [
		["DEEPSEEK_API_KEY", "ATLAS_API_KEY"],
		["DEEPSEEK_BASE_URL", "ATLAS_BASE_URL"],
		["DSH_MODEL", "ATLAS_MODEL"],
	];
	for (const [dst, src] of pairs) {
		if (process.env[dst] === undefined && process.env[src] !== undefined) {
			process.env[dst] = process.env[src];
		}
	}
}

// ---------------------------------------------------------------------------
// ACP client
// ---------------------------------------------------------------------------

interface PendingCall {
	resolve: (result: any) => void;
	reject: (err: Error) => void;
}

interface SessionRecord {
	acpSessionId: string;
	seenLen: number;
}

let proc: ChildProcess | null = null;
let starting: Promise<void> | null = null;
let nextId = 0;
const pending = new Map<number, PendingCall>();
let readBuffer = "";
let turnHandler: ((update: any) => void) | null = null;
const sessions = new Map<string, SessionRecord>();

function resolveAgent(): { command: string; args: string[] } {
	const env = (process.env.DSH_TUI_AGENT ?? "").trim();
	if (env) {
		const toks = env.split(/\s+/);
		return { command: toks[0]!, args: toks.slice(1) };
	}
	const home = homedir();
	const bundledBin = `${home}/.local/lib/node_modules/martty/node_modules/@openma/deepseek-harness-acp/dist/bin.js`;
	const bundledCreator = `${home}/.local/lib/node_modules/martty/creator`;
	if (existsSync(bundledBin) && existsSync(`${bundledCreator}/package.json`)) {
		return { command: bundledBin, args: ["--bundle", bundledCreator] };
	}
	return { command: "dsh-acp", args: [] };
}

function send(obj: unknown): void {
	proc?.stdin?.write(`${JSON.stringify(obj)}\n`);
}

function onAgentLine(line: string): void {
	if (!line.trim()) return;
	let msg: any;
	try {
		msg = JSON.parse(line);
	} catch {
		return;
	}
	if (msg.id !== undefined && pending.has(msg.id)) {
		const call = pending.get(msg.id)!;
		pending.delete(msg.id);
		if (msg.error) call.reject(new Error(`ACP: ${JSON.stringify(msg.error)}`));
		else call.resolve(msg.result ?? null);
		return;
	}
	if (msg.method === "session/update" && turnHandler) {
		try {
			turnHandler(msg.params?.update);
		} catch {
			// display mapping must never kill the turn
		}
	}
}

function acpRpc(method: string, params: unknown): Promise<any> {
	return new Promise((resolve, reject) => {
		const id = ++nextId;
		pending.set(id, { resolve, reject });
		send({ jsonrpc: "2.0", id, method, params });
	});
}

async function acpStart(): Promise<void> {
	if (proc) return;
	if (starting) return starting;
	starting = (async () => {
		ensureCreds();
		const { command, args } = resolveAgent();
		const child = spawn(command, args, { stdio: ["pipe", "pipe", "ignore"] });
		proc = child;
		child.stdout?.setEncoding("utf8");
		child.stdout?.on("data", (chunk: string) => {
			readBuffer += chunk;
			let idx: number;
			while ((idx = readBuffer.indexOf("\n")) >= 0) {
				const line = readBuffer.slice(0, idx);
				readBuffer = readBuffer.slice(idx + 1);
				onAgentLine(line);
			}
		});
		child.on("exit", () => {
			proc = null;
			starting = null;
			for (const call of pending.values()) call.reject(new Error("agent exited"));
			pending.clear();
		});
		await acpRpc("initialize", {
			protocolVersion: 1,
			clientCapabilities: { fs: { readTextFile: true, writeTextFile: true }, terminal: true },
		});
		const key = process.env.DEEPSEEK_API_KEY;
		if (key?.trim()) {
			try {
				await acpRpc("authenticate", {
					methodId: "api-key",
					_meta: { "api-key": { apiKey: key } },
				});
			} catch (e) {
				// env-shadowed ("read-only") means the agent already resolves
				// the key — proceed either way.
				if (!/shadowed|read-only/.test(String(e))) throw e;
			}
		}
	})();
	return starting;
}

/** Idempotent: lets headless runs exit instead of lingering on agent pipes. */
export function shutdownDshAgent(): void {
	try {
		if (proc && !proc.killed) proc.kill();
	} catch {
		// already gone
	}
	proc = null;
	starting = null;
	readBuffer = "";
	turnHandler = null;
	for (const call of pending.values()) {
		try {
			call.reject(new Error("agent shut down"));
		} catch {
			// ignore
		}
	}
	pending.clear();
}

async function acpSession(piSessionId: string): Promise<SessionRecord> {
	await acpStart();
	const existing = sessions.get(piSessionId);
	if (existing) return existing;
	const res = await acpRpc("session/new", { cwd: process.cwd(), mcpServers: [] });
	if (!res?.sessionId) throw new Error(`session/new returned no sessionId: ${JSON.stringify(res)}`);
	const rec: SessionRecord = { acpSessionId: res.sessionId, seenLen: 0 };
	sessions.set(piSessionId, rec);
	return rec;
}

// ---------------------------------------------------------------------------
// transcript helpers
// ---------------------------------------------------------------------------

function blockText(content: unknown): string {
	if (typeof content === "string") return content;
	if (!Array.isArray(content)) return "";
	return (content as Array<{ type?: string; text?: string }>)
		.filter((p) => p && (p.type === "text" || typeof p.text === "string"))
		.map((p) => p.text ?? "")
		.join("\n");
}

function newUserText(messages: TranscriptContext["messages"], rec: SessionRecord): string {
	const start = messages.length < rec.seenLen ? 0 : rec.seenLen;
	const fresh: string[] = [];
	for (const m of messages.slice(start)) {
		if ((m as { role?: string }).role === "user") {
			const t = blockText((m as { content?: unknown }).content).trim();
			if (t) fresh.push(t);
		}
	}
	if (!fresh.length) {
		for (let i = messages.length - 1; i >= 0; i--) {
			const m = messages[i] as { role?: string; content?: unknown };
			if (m?.role === "user") {
				const t = blockText(m.content).trim();
				if (t) {
					fresh.push(t);
					break;
				}
			}
		}
	}
	rec.seenLen = messages.length;
	return fresh.join("\n\n");
}

const ZERO_USAGE = {
	input: 0,
	output: 0,
	cacheRead: 0,
	cacheWrite: 0,
	totalTokens: 0,
	cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

function errorEvent(model: Model<string>, output: AssistantMessage, text: string) {
	output.stopReason = "error";
	output.errorMessage = text;
	return { type: "error", reason: "error", error: { ...output } } as const;
}

// ---------------------------------------------------------------------------
// streams (stream + streamSimple share one core; Pi dispatches on model.api)
// ---------------------------------------------------------------------------

function streamDshTurn(
	model: Model<string>,
	context: TranscriptContext,
	options?: SimpleStreamOptions | StreamOptions,
): AssistantMessageEventStream {
	const stream = createAssistantMessageEventStream();
	const output: AssistantMessage = {
		role: "assistant",
		content: [],
		api: model.api,
		provider: model.provider,
		model: model.id,
		usage: { ...ZERO_USAGE },
		stopReason: "pending",
		timestamp: Date.now(),
	};

	(async () => {
		let textBlock: TextContent | null = null;
		let thinkBlock: ThinkingContent | null = null;
		let textIdx = -1;
		let thinkIdx = -1;
		let textFinal = "";
		let thinkFinal = "";
		const indexOf = (b: TextContent | ThinkingContent) => output.content.indexOf(b);
		const ensureText = (): TextContent => {
			if (!textBlock) {
				textBlock = { type: "text", text: "" };
				output.content.push(textBlock);
				textIdx = indexOf(textBlock);
				stream.push({ type: "text_start", contentIndex: textIdx, partial: output });
			}
			return textBlock;
		};
		const ensureThink = (): ThinkingContent => {
			if (!thinkBlock) {
				thinkBlock = { type: "thinking", thinking: "" };
				output.content.push(thinkBlock);
			thinkIdx = indexOf(thinkBlock);
				stream.push({ type: "thinking_start", contentIndex: thinkIdx, partial: output });
			}
			return thinkBlock;
		};
		const pushText = (delta: string): void => {
			const b = ensureText();
			b.text += delta;
			textFinal += delta;
			stream.push({ type: "text_delta", contentIndex: indexOf(b), delta, partial: output });
		};

		try {
			const sessionKey = (options as { sessionId?: string } | undefined)?.sessionId ?? "default";
			const rec = await acpSession(sessionKey);
			const text = newUserText(context.messages ?? [], rec);
			if (!text.trim()) throw new Error("empty prompt: no new user message");

			stream.push({ type: "start", partial: output });

			const turnResult = await new Promise<any>((resolve, reject) => {
				const signal = (options as { signal?: AbortSignal } | undefined)?.signal;
				const onAbort = (): void => {
					send({ jsonrpc: "2.0", method: "session/cancel", params: { sessionId: rec.acpSessionId } });
				};
				signal?.addEventListener("abort", onAbort, { once: true });

				turnHandler = (upd: any): void => {
					if (!upd) return;
					const kind = upd.sessionUpdate as string | undefined;
					if (kind === "agent_message_chunk") {
						const t = upd.content?.text;
						if (typeof t === "string" && t) pushText(t);
					} else if (kind === "agent_thought_chunk") {
						const t = upd.content?.text;
						if (typeof t === "string" && t) {
							const b = ensureThink();
							b.thinking += t;
							thinkFinal += t;
							stream.push({ type: "thinking_delta", contentIndex: indexOf(b), delta: t, partial: output });
						}
					} else if (kind === "tool_call") {
						const title = upd.title ?? "tool";
						const k = upd.kind ? ` ${upd.kind}` : "";
						pushText(`\n● ${title}${k}\n`);
					} else if (kind === "tool_call_update") {
						const st = upd.status as string | undefined;
						if (st === "completed" || st === "failed") {
							const mark = st === "completed" ? "✓" : "✗";
							const texts = Array.isArray(upd.content)
								? upd.content
										.filter((c: any) => typeof c?.text === "string")
										.map((c: any) => c.text as string)
										.join("\n")
								: "";
							const tail = texts.split("\n").slice(-6).join("\n");
							pushText(
								`${mark}\n${tail
								.split("\n")
								.map((l: string) => `│ ${l}`)
									.join("\n")}\n`,
							);
						}
					}
				};

				const id = ++nextId;
				pending.set(id, {
					resolve: (result) => {
						signal?.removeEventListener("abort", onAbort);
						turnHandler = null;
						resolve(result);
					},
					reject: (err) => {
						signal?.removeEventListener("abort", onAbort);
						turnHandler = null;
						reject(err);
					},
				});
				send({
					jsonrpc: "2.0",
					id,
					method: "session/prompt",
					params: { sessionId: rec.acpSessionId, prompt: [{ type: "text", text }] },
				});
			});

			const stop = (turnResult?.stopReason as string | undefined) ?? "end_turn";
			if (textIdx >= 0) {
				stream.push({
					type: "text_end",
					contentIndex: textIdx,
					content: textFinal,
					partial: output,
				});
			}
			if (thinkIdx >= 0) {
				stream.push({
					type: "thinking_end",
					contentIndex: thinkIdx,
					content: thinkFinal,
					partial: output,
				});
			}
			if (stop !== "end_turn") pushText(`\n[turn stopped: ${stop}]`);
			output.stopReason = "stop";
			stream.push({ type: "done", reason: "stop", message: output });
			stream.end();
		} catch (error) {
			turnHandler = null;
			stream.push(errorEvent(model, output, error instanceof Error ? error.message : String(error)));
			stream.end();
		}
	})();

	return stream;
}

export function dshProvider(): Provider<typeof DSH_API> {
	return createProvider({
		id: DSH_PROVIDER_ID,
		name: "DeepSeek Harness",
		auth: { apiKey: { name: "DSH (ambient)", resolve: async () => ({ auth: {} }) } },
		models: [
			{
				id: DSH_MODEL_ID,
				name: "DSH Harness",
				api: DSH_API,
				provider: DSH_PROVIDER_ID,
				baseUrl: DSH_BASE_URL,
				reasoning: true,
				input: ["text"],
				cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
				contextWindow: 128000,
				maxTokens: 16384,
			},
		],
		api: {
			stream: (model, context, options) => streamDshTurn(model, context, options),
			streamSimple: (model, context, options) => streamDshTurn(model, context, options),
		},
	});
}
