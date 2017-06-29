import moment from 'moment'
import AssetType from '../../api/AssetType'
import addSeriesTitleToSeasons from './addSeriesTitleToSeasons'
import searchFields from './searchFields'
import sequelize from './sequelize'
import tableNames from './tableNames'
const toMediaContentKind = assetType => {
  switch (assetType) {
    case AssetType.movie: return 'movie'
    case AssetType.program: return 'program'
    case AssetType.series: return ['series', 'episode']
  }
}
const parseMoment = (any, format) => {
  if (typeof any === 'string') {
    const parsed = moment.utc(any, format)
    if (parsed.isValid()) {
      return parsed
    }
  }
  return null
}
class QueryBuilder {
  constructor (type) {
    this._type = null
    this._title = null
    this._kinds = null
    this._genres = null
    this._productionFrom = null
    this._productionTo = null
    this._sources = null
    this._availableFrom = null
    this._availableTo = null
  }
  addType (type) {
    this._type = type
  }
  addTitle (title) {
    this._title = title
  }
  addKinds (kinds) {
    this._kinds = kinds
  }
  addGenres (genres) {
    this._genres = genres
  }
  addProductionDates (from, to) {
    const format = 'YYYY-MM-DD'
    this._productionFrom = parseMoment(from, format)
    this._productionTo = parseMoment(to, format)
  }
  addSources (sources) {
    this._sources = sources
  }
  addAvailableDates (from, to) {
    this._availableFrom = parseMoment(from)
    this._availableTo = parseMoment(to)
  }
  buildAndExecute () {
    let where = [ `${tableNames.mediaContent}.data->>'adultContent' <> 'true'` ]
    if (this._title !== null) {
      where.push(`EXISTS (SELECT value FROM jsonb_each(${tableNames.mediaContent}.data->'titles') WHERE value::text ILIKE :title)`)
    }
    if (this._kinds) {
      where.push(`${tableNames.mediaContent}.data->>'kind' IN (:kinds)`)
    }
    if (this._genres !== null) {
      where.push(`EXISTS (SELECT 1 FROM jsonb_array_elements(${tableNames.mediaContent}.data->'genres') AS genre WHERE genre->>'value' IN (:genres))`)
    }
    if (this._productionFrom !== null || this._productionTo !== null) {
      let query = [
        'EXISTS (',
        'SELECT 1',
        'FROM make_date(',
        `(${tableNames.mediaContent}.data->'productionDate'->>'year')::int,`,
        `(${tableNames.mediaContent}.data->'productionDate'->>'month')::int,`,
        `(${tableNames.mediaContent}.data->'productionDate'->>'day')::int`,
        ') AS date',
        `WHERE json_typeof((${tableNames.mediaContent}.data->'productionDate')::json) != 'null'`
      ]
      if (this._productionFrom !== null) {
        query.push('AND date >= :productionFrom::date')
      }
      if (this._productionTo !== null) {
        query.push('AND date <= :productionTo::date')
      }
      query.push(')')
      where.push(query.join(' '))
    }
    if (this._sources !== null) {
      where.push(tableNames.asset + '.channel_id IN (:sources)')
    }
    if (this._availableFrom !== null || this._availableTo !== null) {
      let query = [
        'EXISTS (',
        'SELECT 1',
        'FROM ',
        'LEAST(linear_start) AS start_,',
        'GREATEST(linear_end, buy_end, catchup_end, rent_end, subscription_end) AS end_',
        'WHERE start_ IS NOT NULL AND end_ IS NOT NULL'
      ]
      if (this._availableFrom !== null) {
        query.push('AND start_::timestamp <= :availableFrom::timestamp')
        if (this._availableTo === null) {
          query.push(`AND (end_::timestamp >= :availableFrom::timestamp OR (catchup_end IS NULL AND ${tableNames.asset}.data @> '{"restrictions": {"allowCatchup": true}}'))`)
        }
      }
      if (this._availableTo !== null) {
        query.push(`AND (end_::timestamp >= :availableTo::timestamp OR (catchup_end IS NULL AND ${tableNames.asset}.data @> '{"restrictions": {"allowCatchup": true}}'))`)
        if (this._availableFrom === null) {
          query.push('AND start_::timestamp <= :availableTo::timestamp')
        }
      }
      query.push(')')
      where.push(query.join(' '))
    }
    let query = [
      `SELECT ${searchFields}`,
      `FROM ${tableNames.mediaContent} LEFT OUTER JOIN ${tableNames.asset} ON ${tableNames.mediaContent}.id = ${tableNames.asset}.content_id`
    ]
    if (where.length > 0) {
      query.push('WHERE', where.join(' AND '))
    }
    query.push('LIMIT 48') // fits the maximum size of the dialog nicely
    // TODO: Let the client decide on start and limit (within reasonable limits)
    const dateFormat = 'YYYY-MM-DD'
    const timestampFormat = 'YYYY-MM-DD HH:mm:ss'
    let replacements = {
      availableFrom: this._availableFrom ? this._availableFrom.format(timestampFormat) : null,
      availableTo: this._availableTo ? this._availableTo.format(timestampFormat) : null,
      kinds: this._kinds, // this._type ? toMediaContentKind(this._type) : null,
      genres: this._genres,
      productionFrom: this._productionFrom ? this._productionFrom.format(dateFormat) : null,
      productionTo: this._productionTo ? this._productionTo.format(dateFormat) : null,
      sources: this._sources,
      title: this._title ? ('%' + this._title + '%') : null
    }
    return Promise.await(sequelize.query(query.join(' '), {
      replacements,
      type: sequelize.QueryTypes.SELECT
    }))
  }
}
const findLowestLevelAssets = ({ availableDates, kinds, genres, productionDates, sources, title, type }) => {
  let builder = new QueryBuilder()
  if (typeof type === 'string' && type.length > 0) {
    builder.addType(type)
  }
  if (typeof title === 'string' && title.length > 0) {
    builder.addTitle(title)
  }
  if (Array.isArray(kinds) && kinds.length > 0) {
    builder.addKinds(kinds)
  }
  if (Array.isArray(genres) && genres.length > 0) {
    builder.addGenres(genres)
  }
  if (typeof productionDates === 'object' && productionDates !== null && (typeof productionDates.from === 'string' || typeof productionDates.to === 'string')) {
    builder.addProductionDates(productionDates.from, productionDates.to)
  }
  if (Array.isArray(sources) && sources.length > 0) {
    builder.addSources(sources)
  }
  if (typeof availableDates === 'object' && availableDates !== null && (typeof availableDates.from === 'string' || typeof availableDates.to === 'string')) {
    builder.addAvailableDates(availableDates.from, availableDates.to)
  }
  return builder.buildAndExecute()
}
const getParentIds = assets => {
  let ids = []
  for (let asset of assets) {
    let id = asset.parent_id
    if (id) {
      ids.push(id)
    }
  }
  return ids
}
const getAssetsByContentIds = ids => Promise.await(sequelize.query([
  'SELECT ' + searchFields,
  'FROM ' + tableNames.mediaContent,
  'LEFT OUTER JOIN ' + tableNames.asset + ' ON ' + tableNames.mediaContent + '.id = ' + tableNames.asset + '.content_id',
  'WHERE ' + tableNames.mediaContent + '.id IN (:ids)',
  `ORDER BY ${tableNames.asset}.data->>'kind' DESC`
].join(' '), {
  replacements: { ids },
  type: sequelize.QueryTypes.SELECT
}))
export default Promise.async(search => {
  const maxIterations = 100
  let assets = findLowestLevelAssets(search)
  let newAssets = assets
  let iteration = 0
  while (iteration < maxIterations) {
    ++iteration
    let parentIds = getParentIds(newAssets)
    if (parentIds.length > 0) {
      newAssets = getAssetsByContentIds(parentIds)
      assets = newAssets.concat(assets)
    } else {
      break
    }
  }
  return addSeriesTitleToSeasons(assets)
})
