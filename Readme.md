## Project Description

This project is a Node.js application that integrates with AWS SQS and CloudWatch to monitor and report on the status of SQS queues. It uses the `@aws-sdk/client-sqs` and `@aws-sdk/client-cloudwatch` packages to interact with AWS services and `prom-client` to create custom Prometheus metrics. The application provides an HTTP server that responds to `/ping` requests and serves Prometheus metrics at the `/metrics` endpoint.

## Running the Project

To run this project, you will need Node.js and npm installed on your machine. Follow these steps:

1. Clone the repository to your local machine.
2. Navigate to the project directory in your terminal.
3. Install the dependencies by running `npm install`.
4. Set the required environment variables:
   - `AWS_ACCESS_KEY_ID`: Your AWS access key ID.
   - `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key.
   - `AWS_REGION`: The AWS region your SQS queues are located in.
   - `AWS_PROFILE`: (Optional) The AWS profile to use from your credentials file.
   - `SQS_QUEUES`: A comma-separated list of SQS queue names to monitor.
   - `PORT`: (Optional) The port number for the HTTP server (default is 3001).
5. Start the server by running `npm start` or `node app.js`.
6. Access the `/ping` endpoint in your browser or using a tool like `curl` to check if the server is running.
7. Access the `/metrics` endpoint to view the Prometheus metrics.

Make sure you have the necessary AWS permissions to access SQS and CloudWatch.
