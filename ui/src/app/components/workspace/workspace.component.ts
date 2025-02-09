import { Component, OnDestroy, OnInit } from '@angular/core'
import { DataService } from 'src/app/services/data/data.service'
import { ConversionService } from '../../services/conversion/conversion.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import IConv from '../../model/conv'
import { Subscription } from 'rxjs/internal/Subscription'
import { MatDialog } from '@angular/material/dialog'
import IFkTabData from 'src/app/model/fk-tab-data'
import IColumnTabData, { IIndexData } from '../../model/edit-table'
import ISchemaObjectNode, { FlatNode } from 'src/app/model/schema-object-node'
import { ObjectExplorerNodeType, StorageKeys } from 'src/app/app.constants'
import { IUpdateTableArgument } from 'src/app/model/update-table'
import ConversionRate from 'src/app/model/conversion-rate'

@Component({
  selector: 'app-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
})
export class WorkspaceComponent implements OnInit, OnDestroy {
  conv!: IConv
  fkData: IFkTabData[] = []
  currentObject: FlatNode | null
  tableData: IColumnTabData[] = []
  indexData: IIndexData[] = []
  typeMap: Record<string, Record<string, string>> | boolean = false
  conversionRates: Record<string, string> = {}
  typemapObj!: Subscription
  convObj!: Subscription
  converObj!: Subscription
  ddlsumconvObj!: Subscription
  ddlObj!: Subscription
  isLeftColumnCollapse: boolean = false
  isRightColumnCollapse: boolean = true
  ddlStmts: any
  isOfflineStatus: boolean = false
  spannerTree: ISchemaObjectNode[] = []
  srcTree: ISchemaObjectNode[] = []
  issuesAndSuggestionsLabel: string = 'ISSUES AND SUGGESTIONS'
  objectExplorerInitiallyRender: boolean = false
  srcDbName: string = localStorage.getItem(StorageKeys.SourceDbName) as string
  conversionRatePercentages: ConversionRate = { good: 0, ok: 0, bad: 0 }
  constructor(
    private data: DataService,
    private conversion: ConversionService,
    private dialog: MatDialog,
    private sidenav: SidenavService
  ) {
    this.currentObject = null
  }

  ngOnInit(): void {
    this.ddlsumconvObj = this.data.getRateTypemapAndSummary()

    this.typemapObj = this.data.typeMap.subscribe((types) => {
      this.typeMap = types
    })

    this.ddlObj = this.data.ddl.subscribe((res) => {
      this.ddlStmts = res
    })

    this.convObj = this.data.conv.subscribe((data: IConv) => {
      const indexAddedOrRemoved = this.isIndexAddedOrRemoved(data)
      this.conv = data
      if (indexAddedOrRemoved && this.conversionRates) this.reRenderObjectExplorerSpanner()
      if (!this.objectExplorerInitiallyRender && this.conversionRates) {
        this.reRenderObjectExplorerSpanner()
        this.reRenderObjectExplorerSrc()
        this.objectExplorerInitiallyRender = true
      }
      if (this.currentObject && this.currentObject.type === ObjectExplorerNodeType.Table) {
        this.fkData = this.currentObject
          ? this.conversion.getFkMapping(this.currentObject.name, data)
          : []

        this.tableData = this.currentObject
          ? this.conversion.getColumnMapping(this.currentObject.name, data)
          : []
      }
      if (
        this.currentObject &&
        this.currentObject?.type === ObjectExplorerNodeType.Index &&
        !indexAddedOrRemoved
      ) {
        this.indexData = this.conversion.getIndexMapping(
          this.currentObject.parent,
          this.conv,
          this.currentObject.name
        )
      }
    })

    this.converObj = this.data.conversionRate.subscribe((rates: any) => {
      this.conversionRates = rates
      this.updateConversionRatePercentages()

      if (this.conv) {
        this.reRenderObjectExplorerSpanner()
        this.reRenderObjectExplorerSrc()
        this.objectExplorerInitiallyRender = true
      } else {
        this.objectExplorerInitiallyRender = false
      }
    })

    this.data.isOffline.subscribe({
      next: (res: boolean) => {
        this.isOfflineStatus = res
      },
    })
  }

  ngOnDestroy(): void {
    this.typemapObj.unsubscribe()
    this.convObj.unsubscribe()
    this.ddlObj.unsubscribe()
    this.ddlsumconvObj.unsubscribe()
  }

  updateConversionRatePercentages() {
    const conversionRateCount: ConversionRate = { good: 0, ok: 0, bad: 0 }
    let tableCount: number = Object.keys(this.conversionRates).length
    for (const rate in this.conversionRates) {
      if (this.conversionRates[rate] === 'GRAY' || this.conversionRates[rate] === 'GREEN') {
        conversionRateCount.good += 1
      } else if (this.conversionRates[rate] === 'BLUE' || this.conversionRates[rate] === 'YELLOW') {
        conversionRateCount.ok += 1
      } else {
        conversionRateCount.bad += 1
      }
    }
    if (tableCount > 0) {
      for (let key in this.conversionRatePercentages) {
        this.conversionRatePercentages[key as keyof ConversionRate] = Number(
          ((conversionRateCount[key as keyof ConversionRate] / tableCount) * 100).toFixed(2)
        )
      }
    }
  }

  reRenderObjectExplorerSpanner() {
    this.spannerTree = this.conversion.createTreeNode(this.conv, this.conversionRates)
  }
  reRenderObjectExplorerSrc() {
    this.srcTree = this.conversion.createTreeNodeForSource(this.conv, this.conversionRates)
  }

  reRenderSidebar() {
    this.reRenderObjectExplorerSpanner()
  }

  changeCurrentObject(object: FlatNode) {
    if (object.type === ObjectExplorerNodeType.Table) {
      this.currentObject = object
      this.tableData = this.currentObject
        ? this.conversion.getColumnMapping(this.currentObject.name, this.conv)
        : []

      this.fkData = []
      this.fkData = this.currentObject
        ? this.conversion.getFkMapping(this.currentObject.name, this.conv)
        : []
    } else {
      this.currentObject = object
      this.indexData = this.conversion.getIndexMapping(object.parent, this.conv, object.name)
    }
  }

  updateIssuesLabel(count: number) {
    setTimeout(() => {
      this.issuesAndSuggestionsLabel = `ISSUES AND SUGGESTIONS (${count})`
    })
  }

  leftColumnToggle() {
    this.isLeftColumnCollapse = !this.isLeftColumnCollapse
  }

  rightColumnToggle() {
    this.isRightColumnCollapse = !this.isRightColumnCollapse
  }

  openAssessment() {
    this.sidenav.openSidenav()
    this.sidenav.setSidenavComponent('assessment')
  }
  openSaveSessionSidenav() {
    this.sidenav.openSidenav()
    this.sidenav.setSidenavComponent('saveSession')
  }
  downloadSession() {
    var a = document.createElement('a')
    // JS automatically converts the input (64bit INT) to '9223372036854776000' during conversion as this is the max value in JS.
    // However the max value received from server is '9223372036854775807'
    // Therefore an explicit replacement is necessary in the JSON content in the file.
    let resJson = JSON.stringify(this.conv).replace(/9223372036854776000/g, '9223372036854775807')
    a.href = 'data:text/json;charset=utf-8,' + encodeURIComponent(resJson)
    a.download = `${this.conv.SessionName}_${this.conv.DatabaseType}_${this.conv.DatabaseName}.json`
    a.click()
  }

  updateSpannerTable(data: IUpdateTableArgument) {
    this.spannerTree = this.conversion.createTreeNode(
      this.conv,
      this.conversionRates,
      data.text,
      data.order
    )
  }

  updateSrcTable(data: IUpdateTableArgument) {
    this.srcTree = this.conversion.createTreeNodeForSource(
      this.conv,
      this.conversionRates,
      data.text,
      data.order
    )
  }

  isIndexAddedOrRemoved(data: IConv) {
    if (this.conv) {
      let prevIndexCount = 0
      let curIndexCount = 0
      Object.entries(this.conv.SpSchema).forEach((item) => {
        prevIndexCount += item[1].Indexes ? item[1].Indexes.length : 0
      })
      Object.entries(data.SpSchema).forEach((item) => {
        curIndexCount += item[1].Indexes ? item[1].Indexes.length : 0
      })
      if (prevIndexCount !== curIndexCount) return true
      else return false
    }
    return false
  }
}
