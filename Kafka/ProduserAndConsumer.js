
import { Kafka } from 'kafkajs';

const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"]
})




let produser = null;
function CreatePreduser  (){
    if(produser)  return produser ;
    const newProduser = kafka.producer();
    newProduser.connect();
    produser = newProduser;
    return produser;
}

export async function produseMessage (message){
    const producer  = await CreatePreduser();
    await producer.send({
        message: [{key :`messages :- ${Date.now()}` , value:message}],
        topic: "MESSAGES",
    })

    return true;
}

export async function ConsumeMessages () {
    const consumer  = kafka.consumer({groupId:"defalt"});
    await consumer.connect();
    consumer.subscribe({topic:"MESSAGES"});
    await consumer.run({
        autoCommit:true,
        eachMessage: async ({message ,pause }) => {
            if (!message.value) return;
            try{
                const data = JSON.parse(message.value.toString());
                console.log("message from kafka", data);
                //  message on database
                

            }catch(error){
                // any resion database down then pause messges
                pause();
                setTimeout(() =>{
                    consumer.resume([{topic:"MESSAGES"}])

                }, 120*1000)
            }

            },
    });

}