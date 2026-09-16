# LabPAD
To Run:
Prerequisites:
- Docker desktop installed and running.

First start the broker 
  docker compose up -d broker
Then in a separate terminal you can run the receiver and sender
  docker compose run -rm receiver
  docker compose run -rm sender
You will be asked for ip and port, do default is broker/6001

To run multiple instances just open a new terminal and run the same commands.
Note: Multiple brokers not supported.

Rebuild after code changes:
  docker compose build --no-cache <serviceName>
