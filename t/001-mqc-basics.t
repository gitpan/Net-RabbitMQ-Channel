#!/usr/bin/perl

use Class::Easy;

use Test::More qw(no_plan);

use Net::RabbitMQ::Channel;

my $mqc = Net::RabbitMQ::Channel->new (
	1, hosts => {'web-devel2.rian.off' => {user => 'guest', password => 'guest'}}
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

#$xchange->delete ({if_unused => 0, nowait => 1}); # defaults - {if_unused => 1, nowait => 0}

# publishing
$mqc->publish ($routing_key, $message, {exchange => $xchange->name}, {app_id => 'test'});

# fetching
my $msg = $queue->get;

ok defined $msg;

ok $msg->{body} eq $message;

use Data::Dumper;
diag Dumper $msg;