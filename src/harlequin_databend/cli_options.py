from __future__ import annotations

from harlequin.options import TextOption

host = TextOption(
    name="host",
    description=(
        "Specifies the host name of the machine on which the server is running. "
        "If the value begins with a slash, it is used as the directory for the "
        "Unix-domain socket."
    ),
    short_decls=["-h"],
    default="127.0.0.1",
)


port = TextOption(
    name="port",
    description=(
        "Port number to connect to at the server host, or socket file name extension "
        "for Unix-domain connections."
    ),
    short_decls=["-P", "--port"],
    default="8000",
)

user = TextOption(
    name="user",
    description=("databend user name to connect as."),
    short_decls=["-U", "--user"],
)

password = TextOption(
    name="password",
    short_decls=["-p", "--password"],
    description=("Password to be used if the server demands password authentication."),
)


dbname = TextOption(
    name="dbname",
    description=(
        "The database name to use when connecting with the databend query engine."
    ),
    short_decls=["-D"],
    default="postgres",
)


DATABEND_OPTIONS = [
    host,
    port,
    user,
    password,
    dbname,
]
