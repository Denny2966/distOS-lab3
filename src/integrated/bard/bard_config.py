server_ip = '127.0.0.1'
server_port = '8005'

update_show_interval = 2	# the interval between two shows of scores and/or tallies in the standard output; the unit is second
score_update_prob = 0.9	# the probability scores update after 'update_show_interval' second
event_end_prob = 0.05	# the probability an event ends
event_end_prob_incr_per_interval = 0.02	# we think the probability an event ends will increase with time elapsing; this value indicates the increase of probability after each 'update_show_interl' second
