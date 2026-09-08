build-broker:
	docker build -t synapse-broker:latest -f broker/Dockerfile .
	docker save synapse-broker:latest | sudo k3s ctr images import -

build-autoscaler:
	docker build -t synapse-autoscaler:latest -f autoscaler/Dockerfile .
	docker save synapse-autoscaler:latest | sudo k3s ctr images import -