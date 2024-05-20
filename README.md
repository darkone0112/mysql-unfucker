# mysql-unfucker

### The Ultimate Tool for Unfucking Your MySQL Database

**Description:**

Ever had that heart-stopping moment when your server loses power, and you realize your MySQL database is as broken as your dreams of a stress-free life? Welcome to the club! Meet **mysql-unfucker** – the no-nonsense, take-no-prisoners solution for detecting and logging corrupted rows in your MySQL tables.

**Why You'll Love It:**

- **Sick of Corrupted Rows?** So are we. This tool will sniff out corrupted rows like a bloodhound on a mission.
- **Environment-Friendly:** Loads your DB credentials from a `.env` file, because who has time to hardcode that shit?
- **Logs to JSON:** No more cluttering your precious DB with corruption logs. Keep it clean and simple.
- **Command-Line Interface:** Simple enough for your grandmother to use, but powerful enough to make a sysadmin weep with joy.
- **Survival Mode:** Built with the battle-hardened reality of power outages and server crashes in mind. Let's face it, your hardware probably hates you.

**Features:**

- **Table Listing:** Easily list all tables in your database, because finding stuff shouldn't be harder than dealing with corrupted data.
- **Corruption Checking:** Select any table and let mysql-unfucker do its thing. It checks every row, logs the corrupted ones, and keeps you informed.
- **Humor-Filled Experience:** Let's laugh through the pain. If we're going to deal with database corruption, we might as well have some fun.

**Installation:**

```sh
pip install mysql-connector-python python-dotenv
```
**Usage:**

1. Clone this repo.
2. Set up your `.env` file with your DB credentials.
3. Run the script and follow the simple command-line menu.
4. Sit back and watch mysql-unfucker do its magic.

**Example .env File:**

```sh
DB_NAME=your_ddbb_name
DB_USER=your_user
DB_PASSWORD=a_secure_password_maybe
DB_HOST=Ip_Host_etc
DB_PORT=Connection_port_(3306)_default
```

**Running the Script:**

```sh
python detect_corrupted_rows.py
```
**Enjoy the Bliss of a (mostly) Uncorrupted Database:**

Who knew fixing database corruption could be so satisfying?
Why You Need mysql-unfucker:

Because "reliable power" is an oxymoron, and your sanity is worth more than the time it takes to manually find corrupted rows. Embrace the chaos, and let mysql-unfucker bring order to your MySQL mayhem.

**Disclaimer:**

This tool won't fix your existential crises or make your boss less annoying. But it will help you deal with a corrupted MySQL database, which is a pretty good start.
