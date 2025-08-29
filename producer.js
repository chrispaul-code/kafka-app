const { kafka } = require('./client')
const readline = require('readline')

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
})

async function init() {
  const producer = kafka.producer()

  console.log("Connecting Producer")
  await producer.connect()
  console.log("Producer Connected Successfully")

  rl.setPrompt('> ')
  rl.prompt()

  rl.on('line', async (line) => {
    const [riderName, location] = line.split(' ')

    if (!riderName || !location) {
      console.log("⚠️ Please enter input as: <riderName> <location>")
      rl.prompt()
      return
    }

    try {
      await producer.send({
        topic: 'rider-updates',
        messages: [
          {
            partition: location.toLowerCase() === 'north' ? 0 : 1,
            key: 'location update',
            value: JSON.stringify({ name: riderName, loc: location })
          }
        ]
      })

      console.log(`✅ Message sent for ${riderName} at ${location}`)
    } catch (err) {
      console.error("❌ Error sending message:", err)
    }

    rl.prompt()
  }).on('close', async () => {
    console.log("Closing producer connection...")
    await producer.disconnect()
    process.exit(0)
  })
}

init()
