from surrealist import Database, Algorithm

with Database("http://127.0.0.1:8000", 'test', 'test', credentials=('user_db', 'user_db'), use_http=True) as db:
    # on database object we can DEFINE ACCESS
    # https://surrealdb.com/docs/surrealdb/2.x/surrealql/statements/define/access/

    # Examples for DEFINE ACCESS ... JWT

    # DEFINE ACCESS token ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'some_key' DURATION FOR SESSION 1h;
    print(db.define_access_jwt("token").algorithm(Algorithm.HS256, "some_key").duration("1h"))
    # DEFINE ACCESS IF NOT EXISTS token ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'some_key';
    print(db.define_access_jwt("token").algorithm(Algorithm.HS256, "some_key").if_not_exists())
    # DEFINE ACCESS OVERWRITE token ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'some_key';
    print(db.define_access_jwt("token").algorithm(Algorithm.HS256, "some_key").overwrite())

    key = """-----BEGIN PUBLIC KEY-----
MUO52Me9HEB4ZyU+7xmDpnixzA/CUE7kyUuE0b7t38oCh+sQouREqIjLwgHhFdhh3cQAwr6GH07D
ThioYrZL8xATJ3Youyj8C45QnZcGUif5PkpWXDi0HJSoMFekbW6Pr4xuqIqb2LGxGDVJcLZwJ2AS
Gtu2UAfPXbBD3ffiad393M22g1iHM80YaNi+xgswG7qtXE4lR/Lt4s0MeKKX7stdWI1VIsoB+y3i
r/OWUvJPjjDNbAsyy8tQmxydv+FUnLEP9TNT4AhN4DXcJ+XsDtW7OWt4EdSVDeKpGbIMvIrh1Pe+
Nilj8UHNyNDHa2AjK3seMo6CMvaIQJKj5o4xGFblFGwvvPD03SbuQLs1FdRjsZCeWLdYeQ3JDHE9
sFG7DCXlpMJcaYT1mf4XHJ0gPekNLQyewTY3Vxf7FgV3GCNjV20kcDFgJA2+iVW2wSrb+txD1ycE
kbi8jh0pedWwE40VQWaTh/8eAvX7IHWya/AEro25mq+m6vktNZLbvLphhp586kJK3Tdt3YjpkPre
M3nkFWOWurIyKbtIV9JemfwCgt89sNV45dTlnEDEZFFGnIgDnWgx3CUo4XmhICEQU8+tklw9jJYx
iCTjhbIDEBHySSSc/pQ4ftHQmhToTlQeOdEy4LYiaEIgl1X+hzRH1hBYvWlNKe4EY1nMCKcjgt0=
-----END PUBLIC KEY-----"""

    # DEFINE ACCESS token ON DATABASE TYPE JWT ALGORITHM RS256 KEY '-----BEGIN PUBLIC KEY-----
    # MUO52Me9HEB4ZyU+7xmDpnixzA/CUE7kyUuE0b7t38oCh+sQouREqIjLwgHhFdhh3cQAwr6GH07D
    # ThioYrZL8xATJ3Youyj8C45QnZcGUif5PkpWXDi0HJSoMFekbW6Pr4xuqIqb2LGxGDVJcLZwJ2AS
    # Gtu2UAfPXbBD3ffiad393M22g1iHM80YaNi+xgswG7qtXE4lR/Lt4s0MeKKX7stdWI1VIsoB+y3i
    # r/OWUvJPjjDNbAsyy8tQmxydv+FUnLEP9TNT4AhN4DXcJ+XsDtW7OWt4EdSVDeKpGbIMvIrh1Pe+
    # Nilj8UHNyNDHa2AjK3seMo6CMvaIQJKj5o4xGFblFGwvvPD03SbuQLs1FdRjsZCeWLdYeQ3JDHE9
    # sFG7DCXlpMJcaYT1mf4XHJ0gPekNLQyewTY3Vxf7FgV3GCNjV20kcDFgJA2+iVW2wSrb+txD1ycE
    # kbi8jh0pedWwE40VQWaTh/8eAvX7IHWya/AEro25mq+m6vktNZLbvLphhp586kJK3Tdt3YjpkPre
    # M3nkFWOWurIyKbtIV9JemfwCgt89sNV45dTlnEDEZFFGnIgDnWgx3CUo4XmhICEQU8+tklw9jJYx
    # iCTjhbIDEBHySSSc/pQ4ftHQmhToTlQeOdEy4LYiaEIgl1X+hzRH1hBYvWlNKe4EY1nMCKcjgt0=
    # -----END PUBLIC KEY-----';
    print(db.define_access_jwt("token").algorithm(Algorithm.RS256, key))

    # DEFINE ACCESS token_name ON DATABASE TYPE JWT URL 'https://example.com/.well-known/jwks.json';
    print(db.define_access_jwt("token_name").url("https://example.com/.well-known/jwks.json"))

    # DEFINE ACCESS token_name ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret'
    # AUTHENTICATE IF !$auth.enabled {THROW "This user is not enabled";};RETURN $auth;
    raw = 'IF !$auth.enabled {THROW "This user is not enabled";};RETURN $auth'
    print(db.define_access_jwt("token_name").authenticate(raw).algorithm(Algorithm.HS512, "secret"))

    # Examples for DEFINE ACCESS ... RECORD
    create = db.user.create().set(email="$email", passw="crypto::argon2::generate($pass)")
    select = db.user.select().where("email = $email AND crypto::argon2::compare(pass, $pass)")

    # DEFINE ACCESS account ON DATABASE TYPE RECORD SIGNIN SELECT * FROM user WHERE email = $email AND
    # crypto::argon2::compare(pass, $pass) SIGNUP CREATE user SET email = "$email",
    # passw = "crypto::argon2::generate($pass)" DURATION FOR TOKEN 15m, DURATION FOR SESSION 12h;
    print(db.define_access_record("account").signup(create).signin(select).duration_for_token("15m").duration_for_session("12h"))

    # DEFINE ACCESS token ON DATABASE TYPE RECORD ALGORITHM HS256 KEY 'some_key';
    print(db.define_access_record("token").algorithm(Algorithm.HS256, "some_key"))

    # DEFINE ACCESS token ON DATABASE TYPE RECORD ALGORITHM RS256 KEY 'some_key' WITH ISSUER KEY 'issuer_key';
    print(db.define_access_record("token").algorithm(Algorithm.RS256, "some_key", issuer_key="issuer_key"))

    # DEFINE ACCESS user ON DATABASE TYPE RECORD AUTHENTICATE
    # IF !$auth.enabled {THROW "This user is not enabled";};RETURN $auth; ALGORITHM HS512 KEY 'secret';
    raw = 'IF !$auth.enabled {THROW "This user is not enabled";};RETURN $auth;'
    print(db.define_access_record("user").authenticate(raw).algorithm(Algorithm.HS512, "secret"))

    # You can remove any type of ACCESS with
    print(db.remove_access("token"))  # REMOVE ACCESS token ON DATABASE;

    # Examples for DEFINE ACCESS ... BEARER
    # DEFINE ACCESS api ON DATABASE TYPE BEARER FOR USER DURATION FOR GRANT 30d, FOR TOKEN 15m, FOR SESSION 12h;
    print(db.define_access_bearer("api").type_user().duration_for_grant("30d").duration_for_token("15m").duration_for_session("12h"))
    # DEFINE ACCESS api ON DATABASE TYPE BEARER FOR RECORD DURATION FOR GRANT 10d;
    print(db.define_access_bearer("api").type_record().duration_for_grant("10d"))
    # DEFINE ACCESS IF NOT EXISTS api ON DATABASE TYPE BEARER;
    print(db.define_access_bearer("api").if_not_exists())






