import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup, Validators } from '@angular/forms'
import { Router } from '@angular/router'
import { Input } from '@angular/core'
import IDbConfig from 'src/app/model/db-config'
import DydbConfig from 'src/app/model/dydb-config'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { DataService } from 'src/app/services/data/data.service'
import { LoaderService } from '../../services/loader/loader.service'
import { InputType, StorageKeys } from 'src/app/app.constants'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import { extractSourceDbName } from 'src/app/utils/utils'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'

@Component({
  selector: 'app-direct-connection',
  templateUrl: './direct-connection.component.html',
  styleUrls: ['./direct-connection.component.scss'],
})
export class DirectConnectionComponent implements OnInit {

  @Input("driver") driver : any = {}; 

  connectForm = new FormGroup({
    dbEngine: new FormControl('', [Validators.required]),
  })

  connectFormSql = new FormGroup({
    hostName: new FormControl('', [Validators.required]),
    port: new FormControl('', [Validators.required, Validators.pattern('^[0-9]+$')]),
    userName: new FormControl('', [Validators.required]),
    password: new FormControl(''),
    dbName: new FormControl('', [Validators.required]),
  })

  connectFormDydb = new FormGroup({
    awsAccessKeyID: new FormControl('', [Validators.required]),
    awsSecretAccessKey: new FormControl('', [Validators.required]),
    awsRegion: new FormControl('', [Validators.required]),
    dydbEndpoint: new FormControl('', [Validators.required]),
    schemaSampleSize: new FormControl('', [Validators.required, Validators.pattern('^[0-9]+$')]),
  })

  dbEngineList = [
    { value: 'mysql', displayName: 'MYSQL' },
    { value: 'sqlserver', displayName: 'SQL Server' },
    { value: 'oracle', displayName: 'ORACLE' },
    { value: 'postgres', displayName: 'PostgreSQL' },
    { value: 'dynamodb', displayName: 'DynamoDB' },
  ]

  constructor(
    private router: Router,
    private fetch: FetchService,
    private data: DataService,
    private loader: LoaderService,
    private snackbarService: SnackbarService,
    private clickEvent: ClickEventService
  ) {}

  ngOnInit(): void {}

  connectToDb() {
    this.clickEvent.openDatabaseLoader('direct', this.connectForm.value.dbName)
    window.scroll(0, 0)
    this.data.resetStore()
    const { dbEngine } = this.connectForm.value
    if ( dbEngine == "dynamodb") {
      const { awsAccessKeyID, awsSecretAccessKey, awsRegion, dydbEndpoint, schemaSampleSize} = this.connectFormDydb.value
      const config: DydbConfig = { dbEngine, awsAccessKeyID, awsSecretAccessKey, awsRegion, dydbEndpoint, schemaSampleSize }
      this.fetch.connectToDydb(config).subscribe({
        next: (res) => {
          if (res.status == 200) {
            localStorage.setItem(
              StorageKeys.Config,
              JSON.stringify({ dbEngine, awsAccessKeyID, awsSecretAccessKey, awsRegion, dydbEndpoint, schemaSampleSize })
            )
            localStorage.setItem(StorageKeys.Type, InputType.DirectConnect)
            localStorage.setItem(StorageKeys.SourceDbName, extractSourceDbName(dbEngine))
          }
          this.data.getSchemaConversionFromDb()
          this.data.conv.subscribe((res) => {
            this.router.navigate(['/workspace'])
          })
        },
        error: (e) => {
          this.snackbarService.openSnackBar(e.error, 'Close')
        },
      })
    } else {
      const { hostName, port, userName, password, dbName } = this.connectFormSql.value
      const config: IDbConfig = { dbEngine, hostName, port, userName, password, dbName }
      this.fetch.connectToSql(config).subscribe({
        next: (res) => {
          if (res.status == 200) {
            localStorage.setItem(
              StorageKeys.Config,
              JSON.stringify({ dbEngine, hostName, port, userName, password, dbName })
            )
            localStorage.setItem(StorageKeys.Type, InputType.DirectConnect)
            localStorage.setItem(StorageKeys.SourceDbName, extractSourceDbName(dbEngine))
          }
          this.data.getSchemaConversionFromDb()
          this.data.conv.subscribe((res) => {
            this.router.navigate(['/workspace'])
          })
        },
        error: (e) => {
          this.snackbarService.openSnackBar(e.error, 'Close')
        },
      })
    }
  }
}
