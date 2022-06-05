# Aqua Db

<img src="./image/logo.png" alt="aqua_db" width="200"/>

## schemaの準備

`schema.json`がスキーマの定義になります

```sh
cp schema.example.json schema.json
```

DDLはないので`schema.json`を直接編集してテーブルなどを定義します

### schemaの構成

カラムのタイプは以下です

- int
  - i32
- text
  - 255byte
  
## DML

最後のsemicolonは必須です

### select

`select * from`は固定で、`select id from`のようにカラム指定はできません

```
select * from <table_name>;
```

```
// example
select * from users;
```

### insert

`(` `)`前後の空白は必須です
カラムタイプがtextの場合、`'`で囲う必要があります

```
insert into <table_name> ( column_name1=value1 column_name2=value2 ... )
```

```
// example
insert into users ( name='Mike' id=1 )
```

## start

serverの立ち上げ

```sh
cargo run --bin aqua_db
```

clientの立ち上げ

```sh
cargo run --bin client
```
