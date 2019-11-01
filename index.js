const util = require('util')
const exec = util.promisify(require('child_process').exec)

const nano = require('nano')('http://localhost:5984')

async function createDbIfNotExist(dbname) {
    const dbs = await nano.db.list()
    console.log('dbs', dbs)

    if (!dbs.includes(dbname)) {
        await nano.db.create(dbname)
    }
}

(async function () {
    let db;
    try {
        //
        // Balances
        //

        // Create DB if not exist
        createDbIfNotExist('boobank-list')
        db = nano.use('boobank-list')

        // Get balances thanks to boobank
        let {stdout: accounts} = await exec('boobank list -f json')
        accounts = JSON.parse(accounts);
        console.log('accounts', accounts)
        const accountIds = accounts.map(account => account.id)

        // Get revs
        const revs = await db.fetchRevs({keys: accountIds})
        console.log('revs', revs);

        // upsert (http://docs.couchdb.org/en/stable/api/database/bulk-api.html#db-bulk-docs)
        const docs = accounts.map(account => {
            const doc = {
                ...account,
                _id: account.id
            };

            // Add _rev if any
            const row = revs.rows.find(row => row.key === account.id);
            if (row.value) {
                doc._rev = row.value.rev;
            }

            return doc;
        })
        await db.bulk({docs});

        //
        // Operations
        //

        // Create DB if not exist
        createDbIfNotExist('boobank')
        db = nano.use('boobank')

        // For each account (sequentially)
        for (const account of accounts) {
            // Get operations thanks to boobank
            const daysback = 60;
            const cmd = `boobank history ${account.id} ${new Date(new Date().getTime() - 1000*3600*24*daysback).toISOString().split('T')[0]} -f json`
            let {stdout: operations} = await exec(cmd)
            operations = JSON.parse(operations);
            console.log('operations', operations);

            // Generate an _id from datas
            let operationsIds = [];
            operations.forEach((op, i) => {
                op._id = require('crypto').createHash('md5').update(`${op.id}-${op.date}-${op.amount}-${op.raw}`).digest("hex");
                operationsIds.push(op._id);
            })

            // Get revs
            const revs = await db.fetchRevs({keys: operationsIds})
            console.log('revs', revs);

            // upsert
            const docs = operations.map(op => {
                const doc = {
                    ...op
                };

                // Add _rev if any
                const row = revs.rows.find(row => row.key === op._id);
                if (row.value) {
                    doc._rev = row.value.rev;
                }

                return doc;
            })
            await db.bulk({docs});
        }
    } catch(e) {
        console.log(e)
        process.exit(1)
    }
}).call(this)
