import type { ReactNode } from "react";

interface ToastProps {
  message: ReactNode;
  visible: boolean;
}

export function Toast({ message, visible }: ToastProps) {
  return <div className={`toast${visible ? " show" : ""}`}>{message}</div>;
}
