import enum

class NegotiateOptions(enum.IntFlag):
    none = enum.auto()
    use_sspi = enum.auto()
    use_gssapi = enum.auto()
    use_negotiate = enum.auto()
    use_ntlm = enum.auto()
    negotiate_kerberos = enum.auto()
    session_key = enum.auto()
    wrapping_iov = enum.auto()
    wrapping_winrm = enum.auto()
