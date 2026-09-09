build-broker:
	docker build -t synapse-broker:latest -f broker/Dockerfile .
	docker save synapse-broker:latest | sudo k3s ctr images import -