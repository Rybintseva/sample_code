import json
import logging

import pika

from helpers.constants import RMQ_HOST, RMQ_USER, RMQ_PASSWORD, RMQ_PORT, \
    WORKFLOW_OUTPUT_QUEUE


class CleanseClient:
    """Class for sending/receiving data from services."""
    def __init__(self,
                 rabbitmq_host=RMQ_HOST,
                 rabbitmq_user=RMQ_USER,
                 rabbitmq_password=RMQ_PASSWORD,
                 rabbitmq_port=RMQ_PORT):
        credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(rabbitmq_host, rabbitmq_port,
                                      credentials=credentials))
        self.channel = self.connection.channel()

    def send_data_to_queue(self, queue: str, data: str):
        """
        Publishes to the channel with the given exchange, routing key/queue,
        and body/data. Default exchange is an empty string.
        Args:
            queue: The queue name.
            data: The message body.
        """
        if isinstance(data, dict):
            data = json.dumps(data)
        elif isinstance(data, str):
            pass
        else:
            raise Exception("Data type is not supported. Use str or dict.")
        logging.info(f"Sending data {data} to queue {queue}.")
        self.channel.basic_publish(exchange='',
                                   routing_key=queue,
                                   body=data)

    def receive_data_from_queue(self, queue: str, key: str = None):
        """
        Receives data from queue.
        Args:
            queue: The queue name from which to get a message.
            key: Key data to retrieve.

        Returns:
            Body data by the key.
        """
        method, properties, body = self.channel.basic_get(queue=queue,
                                                          auto_ack=True)
        if any(data is None for data in [method, properties, body]):
            raise Exception(f"The queue {queue} is empty.")
        if key is not None:
            body = body.decode()
            body = json.loads(body)[0]
            data = str(body['data'][key])
            try:
                exception = body['exceptions'][0]['type']
            except IndexError:
                exception = 'None'
            return data, exception
        else:
            return body

    def clear_output(self):
        """Clears the output from RabbitMQ workflow."""
        while True:
            result = self.receive_data_from_queue(WORKFLOW_OUTPUT_QUEUE)
            if not result or result[0] is None:
                return
