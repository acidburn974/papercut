create table if not exists group_table_name
(
    id           bigint auto_increment
        primary key,
    `from`       text     not null,
    `references` text     null,
    message_id   text     not null,
    thread_id    bigint   null,
    parent_id    bigint   null,
    subject      text     not null,
    body         longtext not null,
    created_at   datetime not null,
    updated_at   datetime not null
);

create table if not exists newsgroups
(
    id          int auto_increment
        primary key,
    group_name  varchar(255)      not null,
    table_name  varchar(255)      not null,
    is_active   tinyint default 1 not null,
    description text              null,
    created_at  datetime          not null,
    updated_at  datetime          not null
);

create table if not exists users
(
    id         bigint auto_increment
        primary key,
    username   varchar(255) not null,
    email      varchar(255) not null,
    password   varchar(255) not null,
    first_name varchar(255) not null,
    surname    varchar(255) null,
    created_at datetime     not null,
    updated_at datetime     not null,
    constraint users_username_uindex
        unique (username)
);

