remote_server_ips = ('127.0.0.1', '127.0.0.1')
remote_server_ports = (8005, 8006)
assigned_server_index = 1 # in real system, client is distributed by a load balancing server in general; here I just simulate the balancing policy.

process_id = 3

client_addr = ('127.0.0.1', 7003)

poisson_lambda = 5
simu_len = 60
get_score_pb = 0.8
