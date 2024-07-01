const nodemailer = require("nodemailer");
const sqlite3 = require("sqlite3").verbose();
const fs = require("fs");
const csv = require("csv-parser");
const moment = require("moment-timezone");

// Configuration
const MAX_SEND_PER_HOUR = 2; // Maximum emails per hour for each account
const EMAIL_ACCOUNTS_FILE = "email_accounts.json";
const DATABASE_FILE = "email_stats.db";
const CONTACTS_FILE = "contacts.csv";
const SENDING_WINDOW_START = 8; // 8 AM
const SENDING_WINDOW_END = 16; // 4 PM

// Initialize SQLite database
const db = new sqlite3.Database(DATABASE_FILE);

// Create table if not exists (now includes account_email)
db.run(`CREATE TABLE IF NOT EXISTS email_stats (
  hour INTEGER,
  account_email TEXT,
  sent_count INTEGER DEFAULT 0,
  PRIMARY KEY (hour, account_email)
)`);

// Read email accounts from JSON file
const emailAccounts = JSON.parse(fs.readFileSync(EMAIL_ACCOUNTS_FILE, "utf8"));

// Initialize nodemailer transporter (configure for each account in the sendEmail function)

// Function to read contacts from CSV file
function readContacts() {
  return new Promise((resolve, reject) => {
    const contacts = [];
    fs.createReadStream(CONTACTS_FILE)
      .pipe(csv())
      .on("data", (data) => contacts.push(data))
      .on("end", () => resolve(contacts))
      .on("error", (error) => reject(error));
  });
}

// Function to check if current time is within allowed sending time for a contact
function isAllowedSendingTime(contactTimezone) {
  const contactTime = moment().tz(contactTimezone);
  const contactHour = contactTime.hour();
  return (
    contactHour >= SENDING_WINDOW_START && contactHour < SENDING_WINDOW_END
  );
}

// Function to get sent count for current hour and account
function getSentCount(accountEmail, callback) {
  const currentHour = new Date().getUTCHours();
  db.get(
    "SELECT sent_count FROM email_stats WHERE hour = ? AND account_email = ?",
    [currentHour, accountEmail],
    (err, row) => {
      if (err) {
        console.error("Error getting sent count:", err);
        callback(0);
      } else {
        callback(row ? row.sent_count : 0);
      }
    }
  );
}

// Function to update sent count for an account
function updateSentCount(accountEmail, count) {
  const currentHour = new Date().getUTCHours();
  db.run(
    "INSERT OR REPLACE INTO email_stats (hour, account_email, sent_count) VALUES (?, ?, ?)",
    [currentHour, accountEmail, count],
    (err) => {
      if (err) {
        console.error("Error updating sent count:", err);
      }
    }
  );
}

// Function to send email
function sendEmail(account, contact, callback) {
  const transporter = nodemailer.createTransport({
    service: "gmail", // Configure based on the account's email service
    auth: {
      user: account.email,
      pass: account.password,
    },
  });

  const mailOptions = {
    from: account.email,
    to: contact.email,
    subject: `Hello from ${account.name}`,
    text: `Dear ${contact.name},\n\nI hope this email finds you well. I wanted to reach out regarding...`,
  };

  transporter.sendMail(mailOptions, (error, info) => {
    if (error) {
      console.error("Error sending email:", error);
      callback(false);
    } else {
      console.log("Email sent:", info.response);
      callback(true);
    }
  });
}

// Main function to run continuously
async function runEmailSender() {
  try {
    const contacts = await readContacts();
    let currentContactIndex = 0;
    let currentAccountIndex = 0;

    function processNextContact() {
      if (currentContactIndex >= contacts.length) {
        //currentContactIndex = 0; // Reset to beginning of contact list if we've reached the end
        console.log("Reached end of contact list.");
        return;
      }

      const contact = contacts[currentContactIndex];
      currentContactIndex++;

      if (isAllowedSendingTime(contact.timezone)) {
        const account = emailAccounts[currentAccountIndex];

        getSentCount(account.email, (sentCount) => {
          if (sentCount < MAX_SEND_PER_HOUR) {
            sendEmail(account, contact, (success) => {
              if (success) {
                updateSentCount(account.email, sentCount + 1);
              }
              // Move to the next account for the next email
              currentAccountIndex =
                (currentAccountIndex + 1) % emailAccounts.length;
              // Process next contact after a short delay
              setTimeout(processNextContact, 1000);
            });
          } else {
            console.log(
              `Hourly limit reached for account ${account.email}. Moving to next account.`
            );
            currentAccountIndex =
              (currentAccountIndex + 1) % emailAccounts.length;
            // Try again with the next account
            setTimeout(processNextContact, 1000);
          }
        });
      } else {
        console.log(
          `Outside of allowed sending time for contact ${contact.email}. Skipping to next contact.`
        );
        setTimeout(processNextContact, 1000);
      }
    }

    // Start processing contacts
    processNextContact();
  } catch (error) {
    console.error("Error reading contacts:", error);
    setTimeout(runEmailSender, 15 * 60 * 1000); // Retry after 15 minutes
  }
}

// Start the email sender
runEmailSender();
