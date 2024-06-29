import { type EachMessagePayload, Kafka, Producer } from "kafkajs";

const kafka = new Kafka({
    clientId: 'sample-producer',
    brokers: ['localhost:9092'],
});

const producer: Producer = kafka.producer();

const runProducer = async(): Promise<void> => {
    await producer.connect();

    const sendMessage = async (topic: string, message: string): Promise<void> => {
        await producer.send({
            topic,
            messages: [{value: message}],
        });
    };

    const sendNotification = async (topic: string, payload: any): Promise<void> => {
        const message = JSON.stringify(payload);
        await sendMessage(topic, message);
        console.log(`Message to ${topic}: ${message}`);
    };

    const emailPayload = {
        to: 'reciever@example.com',
        from: 'sender@example.com',
        subject: 'Sample Email',
        body: 'This is sample email notification',
    };

    await sendNotification('email-topic', emailPayload);

    const smsPayload = {
        phoneNumber: '123423143',
        message: 'This is a sample SMS notification',
    };

    await sendNotification('sms-topic', smsPayload);

    await producer.disconnect();
};

runProducer()
    .then(() => {
        console.log('Consumer is running');
    })
    .catch((error) => {
        console.error('Failed to run Kafka consumer', error);
    });
