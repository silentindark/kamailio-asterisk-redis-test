import asyncio
import json
import redis.asyncio as redis
from asterisk.ami import AMIClient, SimpleAction   # pip install asterisk-ami (já no container)

r = redis.Redis(host='redis', port=6379, decode_responses=True)

async def ami_send(asterisk_host, tenant, extension, status):
    try:
        ami = AMIClient(address=asterisk_host,port=5038)
        ami.login(username='admin', secret='admin')
        
        state = "NOT_INUSE" if status == "registered" else "UNAVAILABLE"
        custom = f"REG-{tenant}-{extension}"
        
        action = SimpleAction('Setvar',
                              Variable=f"DEVICE_STATE(Custom:{custom})",
                              Value=state)
        #response = await ami.send_action(action)
        future = ami.send_action(action)
        response = future.response

        print(f"✓ {asterisk_host} → Custom:{custom} = {state} | {response}")
        #await ami.logoff()
        future_logoff = ami.logoff()
        response_logoff = future_logoff.response
    except Exception as e:
        print(f"✗ Erro AMI {asterisk_host}: {e}")

async def listener():
    print("Iniciando...")
    pubsub = r.pubsub()
    await pubsub.subscribe("voice_cache:reg-changes")
    print("🔥 Listener Redis iniciado...")

    async for msg in pubsub.listen():
        if msg['type'] == 'message':
            try:
                data = json.loads(msg['data'])
                tenant = data['tenant']
                ext    = data['extension']
                status = data['status']
                
                # Atualiza os DOIS Asterisks
                await ami_send('asterisk1', tenant, ext, status)
                await ami_send('asterisk2', tenant, ext, status)
            except Exception as e:
                print("Erro JSON:", e)

if __name__ == "__main__":
    asyncio.run(listener())