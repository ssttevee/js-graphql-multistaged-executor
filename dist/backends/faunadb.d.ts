import type { ClientConfig, Expr } from "faunadb";
import type { ExecutorBackend } from "../lib";

export default function createExecutorBackend(
  opts?: ClientConfig,
): ExecutorBackend<Expr>;
