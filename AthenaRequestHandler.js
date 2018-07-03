const AWS = require('aws-sdk')

const athena = new AWS.Athena({apiVersion: '2017-05-18', convertResponseTypes: false})


const getUuidByString = require('uuid-by-string')

const queryExecutionsResults = {}

class AthenaRequestHandler {

  /**
   *
   * @param queryExecutionInput {Athena.StartQueryExecutionInput}
   * @param cacheProps {{expiry,key}}
   */
  constructor(queryExecutionInput, cacheProps) {
    /**@type {Athena.StartQueryExecutionInput}*/
    this.queryExecutionInput = queryExecutionInput


    /**@type {Athena.QueryExecution}*/
    this.queryExecutionStatus = {}

    if (cacheProps) {
      const timestampForCache = Math.round(Date.now() / cacheProps.expiry)
      this.queryCacheId = getUuidByString(timestampForCache + cacheProps.key)
      /**@type {string}*/
      this.queryExecutionId = queryExecutionsResults[this.queryCacheId]
    }
  }

  /**
   *
   * @returns {Promise<any[]>}
   */
  async runAndGetResults() {
    await this.runQuery()
    return await this.getQueryResults()
  }


  // noinspection JSUnusedGlobalSymbols
  /**
   *
   * @returns {Promise<void>}
   */
  async runQuery() {

    console.log("query : " + this.queryExecutionInput.QueryString)
    if (this.queryExecutionId) {
      this.queryExecutionStatus.Status = {State: 'SUCCEEDED'}
    } else {
      try {
        const data = await athena.startQueryExecution(this.queryExecutionInput).promise()
        this.queryExecutionStatus.Status = {State: 'RUNNING'}
        this.queryExecutionId = data.QueryExecutionId
        // return this.checkStatus()
      } catch (e) {
        console.log(e, e.stack)
        return Promise.reject(e)
      }
    }
  }

  /**
   *
   * @returns {Promise<Athena.QueryExecution>}
   */
  async checkStatus() {
    const promise = await athena.getQueryExecution({QueryExecutionId: this.queryExecutionId}).promise()

    /**@type {Athena.QueryExecution}*/
    this.queryExecutionStatus = promise.QueryExecution
    return promise.QueryExecution
  }


  async waitForFinish() {
    const wait = (ms) => {
      return new Promise((resolve) => {
        setTimeout(resolve, ms)
      })
    }

    while (this.queryExecutionStatus.Status.State === 'RUNNING') {
      this.queryExecutionStatus = await this.checkStatus()
      console.log(this.queryExecutionStatus.Status)
      const millisecondsToWait = 1000
      wait(millisecondsToWait)
    }

  }

  /**
   *
   * @returns {Promise<any[]>}
   */
  async getQueryResults() {
    this.waitForFinish()
    if (this.queryExecutionStatus.Status.State === 'SUCCEEDED') {
      queryExecutionsResults[this.queryCacheId] = this.queryExecutionId
      console.log(this.queryExecutionStatus)

      const results = await athena.getQueryResults({QueryExecutionId: this.queryExecutionId}).promise()
      return AthenaRequestHandler.parseAthenaDataToJson(results)
    }
  }

  /**
   *
   * @returns {Promise<Athena.RowList>}
   */
  async getRawQueryResults() {
    if (this.queryExecutionStatus.Status.State === 'SUCCEEDED') {
      queryExecutionsResults[this.queryCacheId] = this.queryExecutionId
      console.log(this.queryExecutionStatus)

      const results = await athena.getQueryResults({QueryExecutionId: this.queryExecutionId}).promise()
      return results.ResultSet.Rows
    }
  }

  /**
   *
   * @param data
   * @returns {any[]}
   */
  static parseAthenaDataToJson(data) {
    let rows = data.ResultSet.Rows
    let metadata = rows[0].Data.map((columnData) => columnData.VarCharValue)

    return rows.slice(1).map((row) => {
      let resultRow = {}
      metadata.forEach(function (columnData, colIndex) {
        let cellData = row.Data[colIndex]
        if (Object.keys(cellData).length === 0) {
          resultRow[metadata[colIndex]] = ""
        } else {
          resultRow[metadata[colIndex]] = cellData.VarCharValue
        }
      })
      return resultRow
    })
  }

  /**
   *
   * @returns {Athena}
   */
  static getAthena() {
    return athena
  }

}

module.exports = AthenaRequestHandler

