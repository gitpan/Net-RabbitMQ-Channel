package Net::RabbitMQ::Exchange;

use Class::Easy;

has 'connection';
has 'channel';
has 'exchange_type';
has 'passive';
has 'auto_delete';
has 'name';

sub new {
	my $class   = shift;
	my $channel = shift;
	my $name    = shift;
	my $options = {@_};
	
	if ($channel->_do (
		'exchange_declare', $name, {
			exchange_type => "topic",
			passive => 0, # the exchange will not get declared but an error will be thrown if it does not exist.
			durable => 1, # the exchange will survive a broker restart.
			auto_delete => 0, # the exchange will get deleted as soon as there are no more queues bound to it. Exchanges to which queues have never been bound will never get auto deleted.
			%$options
		}
	)) {
		return bless {
			channel    => $channel,
			name       => $name
		}, $class;
	}
	
	# if channel didn't open, then we died before this string
}

1;