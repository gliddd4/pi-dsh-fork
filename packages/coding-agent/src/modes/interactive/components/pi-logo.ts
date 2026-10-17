import { foregroundAnsi, isAppleTerminalSession, rgbColor } from "@earendil-works/pi-tui";
import { theme } from "../theme/theme.ts";

const CORAL = rgbColor(228, 138, 122);
const YELLOW = rgbColor(234, 182, 93);
const RESET = "\x1b[0m";

/**
 * DSH fork: no Pi logo. The header shows a plain "dsh" wordmark instead.
 */
export function piLogoLines(): [string, string] {
	return ["dsh", ""];
}

/**
 * Whether the terminal renders the half-block logo correctly. Apple Terminal draws gaps between rows and
 * misaligns the half blocks, so it gets the text wordmark instead.
 */
export function supportsPiLogo(): boolean {
	return !isAppleTerminalSession();
}

/** Text wordmark: "dsh" with the former logo's coral and yellow. */
export function piWordmark(): string {
	const mode = theme.getColorMode();
	return `${foregroundAnsi(CORAL, mode)}d${RESET}${foregroundAnsi(YELLOW, mode)}sh${RESET}`;
}
