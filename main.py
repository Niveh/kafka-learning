import kafka_helper
import time

KAFKA_TOPIC = 'test'


def main():
    if not kafka_helper.topic_exists(KAFKA_TOPIC):
        print(f"Topic {KAFKA_TOPIC} does not exist.\nCreating topic...")
        kafka_helper.create_topic(KAFKA_TOPIC)

    else:
        print(f"Topic {KAFKA_TOPIC} exists.")

    task_handler = kafka_helper.TaskHandler()

    task_handler.add_task(kafka_helper.Producer(KAFKA_TOPIC))
    task_handler.add_task(kafka_helper.Consumer(KAFKA_TOPIC))

    task_handler.run_tasks()
    time.sleep(30)

    task_handler.stop_tasks()


if __name__ == "__main__":
    main()
