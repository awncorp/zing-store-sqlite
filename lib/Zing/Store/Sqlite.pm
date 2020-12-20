package Zing::Store::Sqlite;

use 5.014;

use strict;
use warnings;

use registry 'Zing::Types';
use routines;

use Data::Object::Class;
use Data::Object::ClassHas;

extends 'Zing::Store';

# VERSION

# ATTRIBUTES

has client => (
  is => 'ro',
  isa => 'InstanceOf["DBI::db"]',
  new => 1,
);

fun new_client($self) {
  require DBI; DBI->connect(
    join("=", "dbi:SQLite:dbname", $ENV{ZING_DBNAME} || 'zing.db'),
    '', '',
    {
      AutoCommit => 1,
      PrintError => 0,
      RaiseError => 1
    }
  );
}

has meta => (
  is => 'ro',
  isa => 'Str',
  new => 1,
);

fun new_meta($self) {
  require Zing::ID; Zing::ID->new->string
}

has table => (
  is => 'ro',
  isa => 'Str',
  new => 1,
);

fun new_table($self) {
  $ENV{ZING_DBZONE} || 'entities'
}

# BUILDERS

fun new_encoder($self) {
  require Zing::Encoder::Dump; Zing::Encoder::Dump->new;
}

fun BUILD($self) {
  my $client = $self->client;
  my $table = $self->table;
  local $@; eval {
    $client->do(qq{
      create table if not exists "$table" (
        "id" integer primary key,
        "key" varchar not null,
        "value" text not null,
        "index" integer default 0,
        "meta" varchar null
      )
    });
  }
  unless (defined(do{
    local $@;
    local $client->{RaiseError} = 0;
    local $client->{PrintError} = 0;
    eval {
      $client->do(qq{
        select 1 from "$table" where 1 = 1
      })
    }
  }));
  return $self;
}

fun DESTROY($self) {
  $self->client->disconnect;
  return $self;
}

# METHODS

my $retries = 10;

method drop(Str $key) {
  my $table = $self->table;
  my $client = $self->client;
  my $sth = $client->prepare(
    qq{delete from "$table" where "key" = ?}
  );
  $sth->execute($key);
  return $sth->rows > 0 ? 1 : 0;
}

method keys(Str $query) {
  $query =~ s/\*/%/g;
  my $table = $self->table;
  my $client = $self->client;
  my $data = $client->selectall_arrayref(
    qq{select distinct("key") from "$table" where "key" like ?},
    {},
    $query,
  );
  return [map $$_[0], @$data];
}

method lpull(Str $key) {
  my $table = $self->table;
  my $client = $self->client;
  for my $attempt (1..$retries) {
    local $@; eval {
      my $sth = $client->prepare(
        qq{
          update "$table" set "meta" = ? where "id" = (
            select "me"."id" from "$table" "me"
            where "me"."key" = ? and "me"."meta" is null
            order by "me"."index" asc limit 1
          )
        }
      );
      $sth->execute($self->meta, $key);
    };
    if ($@) {
      die $@ if $attempt == $retries;
    }
    else {
      last;
    }
  }
  my $data = $client->selectrow_arrayref(
    qq{
      select "id", "value"
      from "$table" where "meta" = ? and "key" = ? order by "index" asc limit 1
    },
    {},
    $self->meta, $key,
  );
  if ($data) {
    my $sth = $client->prepare(
      qq{delete from "$table" where "id" = ?}
    );
    $sth->execute($data->[0]);
  }
  return $data ? $self->decode($data->[1]) : undef;
}

method lpush(Str $key, HashRef $val) {
  my $table = $self->table;
  my $client = $self->client;
  my $sth = $client->prepare(
    qq{
      insert into "$table" ("key", "value", "index") values (?, ?, (
        select coalesce(min("me"."index"), 0) - 1
        from "$table" "me" where "me"."key" = ?
      ))
    }
  );
  for my $attempt (1..$retries) {
    local $@; eval {
      $sth->execute($key, $self->encode($val), $key);
    };
    if ($@) {
      die $@ if $attempt == $retries;
    }
    else {
      last;
    }
  }
  return $sth->rows;
}

method read(Str $key) {
  my $table = $self->table;
  my $client = $self->client;
  my $data = $client->selectrow_arrayref(
    qq{
      select "value" from "$table"
      where "key" = ? order by "id" desc limit 1
    },
    {},
    $key,
  );
  return $data ? $data->[0] : undef;
}

method recv(Str $key) {
  my $data = $self->read($key);
  return $data ? $self->decode($data) : $data;
}

method rpull(Str $key) {
  my $table = $self->table;
  my $client = $self->client;
  for my $attempt (1..$retries) {
    local $@; eval {
      my $sth = $client->prepare(
        qq{
          update "$table" set "meta" = ? where "id" = (
            select "me"."id" from "$table" "me"
            where "me"."key" = ? and "me"."meta" is null
            order by "me"."index" desc limit 1
          )
        }
      );
      $sth->execute($self->meta, $key);
    };
    if ($@) {
      die $@ if $attempt == $retries;
    }
    else {
      last;
    }
  }
  my $data = $client->selectrow_arrayref(
    qq{
      select "id", "value"
      from "$table" where "meta" = ? and "key" = ? order by "index" desc limit 1
    },
    {},
    $self->meta, $key,
  );
  if ($data) {
    my $sth = $client->prepare(
      qq{delete from "$table" where "id" = ?}
    );
    $sth->execute($data->[0]);
  }
  return $data ? $self->decode($data->[1]) : undef;
}

method rpush(Str $key, HashRef $val) {
  my $table = $self->table;
  my $client = $self->client;
  my $sth = $client->prepare(
    qq{
      insert into "$table" ("key", "value", "index") values (?, ?, (
        select coalesce(max("me"."index"), 0) + 1
        from "$table" "me" where "me"."key" = ?
      ))
    }
  );
  for my $attempt (1..$retries) {
    local $@; eval {
      $sth->execute($key, $self->encode($val), $key);
    };
    if ($@) {
      die $@ if $attempt == $retries;
    }
    else {
      last;
    }
  }
  return $sth->rows;
}

method send(Str $key, HashRef $val) {
  my $set = $self->encode($val);
  $self->write($key, $set);
  return 'OK';
}

method size(Str $key) {
  my $table = $self->table;
  my $client = $self->client;
  my $data = $client->selectrow_arrayref(
    qq{select count("key") from "$table" where "key" = ?},
    {},
    $key,
  );
  return $data->[0];
}

method slot(Str $key, Int $pos) {
  my $table = $self->table;
  my $client = $self->client;
  my $data = $client->selectrow_arrayref(
    qq{
      select "value" from "$table"
      where "key" = ? order by "index" asc limit ?, 1
    },
    {},
    $key, $pos
  );
  return $data ? $self->decode($data->[0]) : undef;
}

method test(Str $key) {
  my $table = $self->table;
  my $client = $self->client;
  my $data = $client->selectrow_arrayref(
    qq{select count("id") from "$table" where "key" = ?},
    {},
    $key,
  );
  return $data->[0] ? 1 : 0;
}

method write(Str $key, Str $data) {
  my $table = $self->table;
  my $client = $self->client;
  $client->prepare(
    qq{delete from "$table" where "key" = ?}
  )->execute($key);
  $client->prepare(
    qq{insert into "$table" ("key", "value") values (?, ?)}
  )->execute($key, $data);
  return $self;
}

1;
