import type { PasswordChange, Profile, TlsSettings } from "../bridge";

/** What the password field means for the stored password. */
export function passwordChange(
  typed: string,
  clearRequested: boolean,
): PasswordChange {
  if (clearRequested) return { kind: "clear" };
  if (typed) return { kind: "set", value: typed };
  return { kind: "keep" };
}

export const emptyTls: TlsSettings = {
  caFile: null,
  clientCertFile: null,
  clientKeyFile: null,
  insecureSkipVerify: false,
};

/** Tidies a draft before saving: trims text, drops empty optional fields and blank Subscriptions. */
export function cleanDraft(draft: Profile): Profile {
  const blankToNull = (value: string | null | undefined) =>
    value?.trim() ? value.trim() : null;
  return {
    ...draft,
    name: draft.name.trim(),
    host: draft.host.trim(),
    clientId: draft.clientId.trim(),
    username: blankToNull(draft.username),
    tls: draft.tls && {
      caFile: blankToNull(draft.tls.caFile),
      clientCertFile: blankToNull(draft.tls.clientCertFile),
      clientKeyFile: blankToNull(draft.tls.clientKeyFile),
      insecureSkipVerify: draft.tls.insecureSkipVerify,
    },
    subscriptions: draft.subscriptions
      .map((s) => ({ ...s, filter: s.filter.trim() }))
      .filter((s) => s.filter),
  };
}
