const { ImapFlow } = require('imapflow')
const transferData = require('./transferEmails.json')

const currentDate = new Date()
const logger = require('logger').createLogger(`./logs/${currentDate.toUTCString()}-run.log`);

function createClient(provider, auth) {
  if (!provider || !provider.host || !provider.port) {
    throw new Error('Provider should be an object with { host: string, port: number }')
  }

  if (!auth || !auth.email || !auth.password) {
    throw new Error('Auth should be an object with { email: string, password: string }')
  }

  return new ImapFlow({
    host: provider.host,
    port: provider.port,
    secure: true,
    tls: {
      minVersion: 'TLSv1'
    },
    auth: {
      user: auth.email,
      pass: auth.password
    }
  })
}

function findMailboxOrCreateIt (mailboxList, mailboxToFind, client) {
  const mailbox = mailboxList.find(
    mailbox => 
      mailbox.name === mailboxToFind.name 
      && mailbox.parent.join('.') === mailboxToFind.parent.join('.')
  )

  if (mailbox) {
    return Promise.resolve(mailbox)
  }

  return client.mailboxCreate([...mailboxToFind.parent, mailboxToFind.name])
}

function downloadMessageAndUpload(msg, mailboxTo, fromClient, toClient) {

  return fromClient
          .download(msg.uid, undefined, { uid: true })
          .then(({content}) => new Promise((resolve, reject) => {
            const buffers = []

            content.on('data', (data) => {
              buffers.push(data)
            })
        
            content.on('end', () => resolve(toClient.append(mailboxTo.path, Buffer.concat(buffers), [...msg.flags], msg.date)))
            content.on('close', () => resolve(toClient.append(mailboxTo.path, Buffer.concat(buffers), [...msg.flags], msg.date)))
        
            content.on('error', (err) => {
              reject(err)
            })
          }))
}

const main = async () => {
  const mailboxesFromEmailsCountMessage = []

  for(let [index, auth] of Object.entries(transferData.from.emails)) {
    logger.info(`Migrating: 
      - From: ${auth.email}
      - To: ${transferData.to.emails[index].email}
    `)

    const fromClient = createClient(transferData.from.provider, auth)
    const toClient = createClient(transferData.to.provider, transferData.to.emails[index])
    
    logger.info(`Created clients.`)

    try {
      logger.info(`Waiting clients to connect...`)

      await fromClient.connect()
      await toClient.connect()

      fromClient.on('close', async () => {
        await fromClient.connect()
      })

      toClient.on('close', async () => {
        await toClient.connect()
      })

      fromClient.on('error', (err) => {
        logger.error('Erro on From client', err)
      })

      toClient.on('error', async () => {
        logger.error('Erro on To client', err)
      })

      logger.info(`Clients connected.`)

      const fromQuota = await fromClient.getQuota()
      const toQuota = await toClient.getQuota()

      if (fromQuota && toQuota && fromQuota.storage.used > toQuota.storage.available) {
        throw new Error('Disk quota used in the From email is bigger than the To email')
      }

      // Get all emails mailboxes
      logger.info(`Getting all mailboxes...`)

      const mailboxesOfFromEmail = await fromClient.list()
      const mailboxesOfToEmail = await toClient.list()

      logger.info(`Got all mailboxes: 
        - From mailboxes: ${JSON.stringify(mailboxesOfFromEmail.map(mailbox => mailbox.path))}
        - To mailboxes: ${JSON.stringify(mailboxesOfToEmail.map(mailbox => mailbox.path))}`)

      // Go through each mailbox of the main email 
      logger.info('Going through all mailboxes')

      const mailboxesCountMessageFromEmail = {
        emailFrom: auth.email,
        emailTo: transferData.to.emails[index].email,
        mailboxesMessages: []
      }

      mailboxLoop: for (const fromMailbox of mailboxesOfFromEmail) {
        if (fromMailbox.flags.has('\\Junk') || fromMailbox.flags.has('\\Trash')) {
          continue mailboxLoop
        }

        logger.info('Mailbox: ', fromMailbox.path)
        
        try {
          const toMailbox = await findMailboxOrCreateIt(mailboxesOfToEmail, fromMailbox, toClient)

          let fromMailboxLock
          let toMailboxLock

          try {
            fromMailboxLock = await fromClient.getMailboxLock(fromMailbox.path)
            toMailboxLock = await toClient.getMailboxLock(toMailbox.path)

            const { messages: numOfMessages } = await fromClient.status(fromMailbox.path, {
              messages: true
            })

            let messagesUploadedCount = 0

            logger.info('Getting messages from mailbox', fromMailbox, 'from message', 1, 'to', '*')

            try {
              const messages = []
              // Getting messages from mailbox
              for await (let msg of fromClient.fetch(`1:*`, {uid: true, internalDate: true, flags: true})){
                messages.push({
                  uid: msg.uid,
                  date: msg.internalDate,
                  flags: msg.flags
                })
              }

              const messagesDownloadAndUploadPromises = []
              for (const message of messages) {
                messagesDownloadAndUploadPromises.push(
                  downloadMessageAndUpload(message, toMailbox, fromClient, toClient)
                )
              }
              
              try {
                const results = await Promise.allSettled(messagesDownloadAndUploadPromises)

                results.forEach((result, index) => {
                  if (result.status === 'fulfilled') {
                    messagesUploadedCount += 1
                  } else {
                    logger.error('Error download and uploading message', messages[index], 'from mailbox', fromMailbox.path, 'to', toMailbox.path, '| Error trace', result.reason)
                  }
                })
              } catch (error) {
                logger.error(error)
              }
            } catch (error) {
              logger.error('Error getting messages from mailbox', fromMailbox.path, 'from message', 1, 'to', '*', '| Error trace', error)
            }

            logger.info('Messages uploaded to mailbox', toMailbox.path, ':', messagesUploadedCount, 'of', numOfMessages)

            mailboxesCountMessageFromEmail.mailboxesMessages.push({
              fromMailbox: fromMailbox.path,
              toMailbox: toMailbox.path,
              totalMessages: numOfMessages,
              messagesUploaded: messagesUploadedCount
            })

            if (messagesUploadedCount !== numOfMessages) {
              logger.error('Messages uploaded doen\'t matches number of total messages of', fromMailbox.path)
            }

          } catch (error) {
            logger.error('Error getting lock for mailboxes: ', fromMailbox.path, '| Error trace:', error)
          } finally {
            if (fromMailboxLock) fromMailboxLock.release()
            if (toMailboxLock) toMailboxLock.release()
          }

        } catch(error) {
          logger.error('Error creating new mailbox: ', fromMailbox.path, '| Error trace: ', error)
        }
      }

      mailboxesFromEmailsCountMessage.push(mailboxesCountMessageFromEmail)

    } catch (error) {
      logger.error('Main error', error)
    } finally {
      if (fromClient.authenticated) {
        fromClient.logout()
      }

      if(toClient.authenticated) {
        toClient.logout()
      }
    }
  }

  const finalLog = mailboxesFromEmailsCountMessage.reduce((prevString, email) => {
    return `
      ${prevString}
    ----------------------------------
      Email from: ${email.emailFrom}
      Email to: ${email.emailTo}
      Mailboxes:
        ${
          email.mailboxesMessages.reduce((mailboxPrevString, mailbox) => {
            return `
              ${mailboxPrevString}
              - Mailbox from: ${mailbox.fromMailbox}
                Mailbox to: ${mailbox.toMailbox}
                Total messages from: ${mailbox.totalMessages}
                Messages uploaded: ${mailbox.messagesUploaded}
            `
          }, '').trim()
        }
    `
  }, '')

  console.log(finalLog)
  logger.info(finalLog)
}

main().catch(err => console.error(err))