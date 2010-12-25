#!/usr/bin/perl

use Class::Easy;

use Test::More qw(no_plan);

use_ok 'Net::RabbitMQ::Channel';

my $host = $ENV{'MQHOST'} || "dev.rabbitmq.com";

# reconnect test

my $hosts = {
	a => {},
	b => {},
	c => {failed => 3},
	d => {failed => 2},
	e => {failed => 1}
};

foreach ([keys %$hosts], [reverse keys %$hosts], [sort keys %$hosts], [reverse sort keys %$hosts]) {
	ok join (', ', sort {Net::RabbitMQ::Channel::_failed_host_sort_sub ($hosts, $a, $b)} @$_) =~ /, c, d, e$/;
}

my $mqc = Net::RabbitMQ::Channel->new (
	1, hosts => {$host => {user => 'guest', password => 'guest'}}
);

ok ($mqc);

my $abc = 'c';

my $xchange = $mqc->exchange_declare (
	"${abc}test_x",
	exchange_type => "topic",
	passive => 0,
	durable => 1,
	auto_delete => 0
);
ok ($xchange);

my $queue   = $mqc->queue_declare (
	"${abc}test_q",
	passive => 0,
	durable => 1,
	exclusive => 0,
	auto_delete => 0
);
ok ($queue);

my $routing_key = "${abc}test_k";

my $message = "$queue";

# before consumption
ok $queue->bind ($xchange, $routing_key);

# publishing
$mqc->publish ($routing_key, $message, exchange => $xchange->name, app_id => 'test');

# fetching
my $msg = $queue->get;

ok defined $msg;

ok $msg->{body} eq $message;

ok $queue->unbind ($xchange, $routing_key);

ok $queue->purge;

# ok $xchange->delete (if_unused => 0, nowait => 1); # defaults - {if_unused => 1, nowait => 0}

#use Data::Dumper;
#diag Dumper $msg;