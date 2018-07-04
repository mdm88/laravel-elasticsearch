<?php
namespace Yong\ElasticSuit\Elasticsearch\Query\Grammar;

use RuntimeException;
use Illuminate\Support\Str;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar as BaseGrammar;

class Grammar extends BaseGrammar
{
    private function notSupport($keyword) {
        throw new RuntimeException(sprintf(' "%s" is not supported by Grammar', $keyword));
    }

    public function __call($method, $args) {
        $this->notSupport($method);
    }

    private function processKeyValue(Builder $query, array &$values) {
        if ($query->keyname && isset($values[$query->keyname])) {
            $keyvalue = $values[$query->keyname];
            // unset($values[$query->keyname]);  //we still keep the
            return [true, $keyvalue];
        }
        return [false, null];
    }

    /**
     * Compile an insert statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $values
     * @return string
     */
    public function compileInsert(Builder $query, array $values)
    {
        list($result, $keyValue) = $this->processKeyValue($query, $values);
        $params = [
            'index'=> $query->getConnection()->getDatabaseName(),
            'type'=> $query->from,
            'body'=> $values
            // 'id' => $docId
        ];
        if ($result) {
            $params['id'] = $keyValue;
        }
        return $params;
    }

    /**
     * Compile a select query into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return string
     */
    public function compileSelect(Builder $query)
    {
        $original = $query->columns;

        if (is_null($query->columns)) {
            $query->columns = [];
        }

        $sqls = $this->compileComponents($query);
        $query->columns = $original;

        $searchs = [
            'index' => $query->getConnection()->getDatabaseName(),
            'type' => $query->from,
            'body' => $sqls
        ];
        return $searchs;
    }

    /**
     * Compile the "fields" portion of the query.
     *
     * @param      \Illuminate\Database\Query\Builder  $query    The query
     * @param      <type>                              $columns  The columns
     *
     * @return     array                               ( description_of_the_return_value )
     */
    protected function compileColumns(Builder $query, $columns)
    {
        if (! is_null($query->aggregate)) {
            return false;
        }

        return $columns;
    }

    /**
     * Compile the "from" portion of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  string  $table
     * @return string
     */
    protected function compileFrom(Builder $query, $table)
    {
        return false;
    }

    protected function compileWheres(Builder $query)
    {
        if (is_null($query->wheres)) {
            return false;
        }
        $query_cond = [
            'must'=>[],
            'must_not'=>[],
            'should'=>[]
        ];
        $filter_cond = [
            'or' => [
                'filters' => [
                    [
                        'and' => [
                            'filters' => [

                            ]
                        ]
                    ]
                ]
            ]
        ];
        $orIndex = 0;

        foreach ($query->wheres as $where) {
            $method = "where{$where['type']}";
            if ($method === 'whereBasic') {
                list($expressions, $is_filter) = $this->$method($query, $where);
            } else {
                $expressions = $this->$method($query, $where);
                $is_filter = ($where['type'] != 'multimatch');
            }

            if (!empty($expressions) && is_array($expressions) && count($expressions)>0) {
                if ($is_filter && $where['boolean'] == 'or' && count($filter_cond['or']['filters'][$orIndex]['and']['filters']) > 0) {
                    // Agrego un nuevo or
                    $filter_cond['or']['filters'][] = [
                        'and' => [
                            'filters' => [

                            ]
                        ]
                    ];
                    $orIndex++;
                }

                switch(strtolower(implode('_', [$where['boolean'], $where['type']]))) {
                    case 'and_basic':
                        if ($is_filter) {
                            $filter_cond['or']['filters'][$orIndex]['and']['filters'][] = $expressions;
                        } else {
                            $query_cond['must'][] = $expressions;
                        }

                        break;
                    case 'or_basic':
                        if ($is_filter) {
                            $filter_cond['or']['filters'][$orIndex]['and']['filters'][] = $expressions;
                        } else {
                            $query_cond['should'][] = $expressions;
                        }
                        break;

                    case 'and_multimatch':
                        $query_cond['must'][] = $expressions;
                        break;

                    case 'and_in':
                    case 'and_null':
                    case 'or_in':
                    case 'or_null':
                    case 'and_notin':
                    case 'and_notnull':
                    case 'or_notin':
                    case 'or_notnull':
                    case 'and_raw':
                    case 'or_raw':
                        $filter_cond['or']['filters'][$orIndex]['and']['filters'][] = $expressions;
                        break;

                    default:
                        $this->notSupport($method);
                        break;
                }
            }
        }

        if (count($query_cond['must']) > 0 || count($query_cond['must_not'] > 0) || count($query_cond['should']) > 0) {
            if (count($query_cond['must']) == 1) {
                $query_cond['must'] = $query_cond['must'][0];
            }
            return [
                'query'=>['bool'=>$query_cond],
                'filter'=>$filter_cond
            ];
        }
        return false;
    }

    private $whereOperatorsMapping = [
        '>' => 'gt',
        '>=' => 'gte',
        '<' => 'lt',
        '<=' => 'lte',
    ];

    protected function removeTableFromColumn(Builder $query, $column)
    {
        if (! Str::contains($column, '.')) {
            return $column;
        }

        list($table, $column) = explode('.', $column);
        if ($table !== $query->from) {
            $this->notSupport('Join table search');
        }
        return $column;
    }

    protected function whereNested(Builder $query, $where)
    {
        $nested = $where['query'];
        $this->compileWheres($nested);
        return $this;
    }

    /**
     * where condition for multiple fields
     *
     * @param      \Illuminate\Database\Query\Builder  $query  The query
     * @param      <type>                              $where  The where
     *
     * @return     array                               ( description_of_the_return_value )
     */
    protected function whereMultiMatch(Builder $query, $where) {
        return [
            'multi_match' => [
                'query' => $where['value'],
                $where['operator'] => $where['op_param'],
                'fields' => $where['columns']
            ]
        ];
    }

    /**
     * Compile a "where null" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereNull(Builder $query, $where)
    {
        return [
            'exists' => [
                'field' => $where['column']
            ]
        ];
    }

    /**
     * Compile a "where not null" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereNotNull(Builder $query, $where)
    {
        return [
            'missing' => [
                'field' => $where['column']
            ]
        ];
    }

    /**
     * Compile a basic where clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereBasic(Builder $query, $where)
    {
        $value = $where['value'];
        $column = $this->removeTableFromColumn($query, $where['column']);

        $is_filter = false;
        $filters = [];
        switch($where['operator']) {
            case '>':
            case '>=':
            case '<':
            case '<=':
                $filters['range'] = [$column => [$this->whereOperatorsMapping[$where['operator']]=>$value]];
                break;
            case 'like':
                $filters['match'] = [
                    $column => [
                        'query' => $value,
                        'fuzziness'=>'AUTO',
                        'operator' => 'and'
                    ]
                ];
                break;
            case '=':
                $is_filter = true;
                $filters = [
                    'term' => [
                        $column => $value
                    ]
                ];
                break;
            case '<>':
            case '!=':
                $is_filter = true;
                $filters = [
                    'not' => [
                        'term' => [
                            $column => $value
                        ]
                    ]
                ];
                break;

            default:
                $this->notSupport('where operator ' . $where['operator']);
                break;
        }

        return [$filters, $is_filter];
    }

    /**
     * Compile a "where in" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereIn(Builder $query, $where)
    {
        if (empty($where['values'])) {
            return false;
        }
        $column = $this->removeTableFromColumn($query, $where['column']);
        return [
            "terms" => [
              $column => $where['values']
            ]
        ];
    }

    /**
     * Compile a "where not in" clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereNotIn(Builder $query, $where)
    {
        return ['not' => $this->whereIn($query, $where)];
    }

    /**
     * Compile a where in sub-select clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $where
     * @return string
     */
    protected function whereInSub(Builder $query, $where)
    {
        $this->notSupport('whereInSub');
    }

    /**
     * Compile the "limit" portions of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  int  $limit
     * @return string
     */
    protected function compileOffset(Builder $query, $offset)
    {
        return $offset;
    }



    /**
     * Compile the "limit" portions of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  int  $limit
     * @return string
     */
    protected function compileLimit(Builder $query, $limit)
    {
        return $limit;
    }


     /**
     * The components that make up a select clause.
     *
     * @var array
     */
    protected $selectComponentsMapping = [
        'aggregate' => 'aggs',
        'columns' => '_source',
        'from' => 'from',
        'joins' => 'joins',
        'wheres' => '',
        'groups' => 'groups',
        'havings' => 'havings',
        'orders' => 'sort',
        'limit' => 'size',
        'offset' => 'from',
        'unions' => 'unions',
        'lock' => 'lock',
    ];
     /**
     * Compile the components necessary for a select clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return array
     */
    protected function compileComponents(Builder $query)
    {
        $sql = [];
        foreach ($this->selectComponents as $component) {
            // To compile the query, we'll spin through each component of the query and
            // see if that component exists. If it does we'll just call the compiler
            // function for the component which is responsible for making the SQL.
            if (! is_null($query->$component)) {
                $method = 'compile'.ucfirst($component);
                $result = $this->$method($query, $query->$component);
                if ($result !== false) {
                    $componentname = $this->selectComponentsMapping[$component];
                    if (!$componentname) {
                        $sql = array_merge($sql, $result);
                    } else {
                        $sql[$componentname] = $result;
                    }
                }
            }
        }
        return $sql;
    }


    private $aggregateMapping = [
        'count'=>'value_count',
        'max'=>'max',
        'min'=>'min',
        'avg'=>'avg',
        'sum'=>'sum',
        'stats'=>'stats'
    ];
    /**
     * Compile an aggregated select clause.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $aggregate
     * @return string
     */
    protected function compileAggregate(Builder $query, $aggregate)
    {
        // return [
        //     "eloquent_aggregate" => [
        //         $this->aggregateMapping[$aggregate['function']] => [ "field" => $aggregate['columns']]
        //     ]
        // ];
        $column = implode(',', $aggregate['columns']);
        $column = ($column=='*') ? $query->keyname : $column;
        return [
            'aggregate' => [
                $this->aggregateMapping[$aggregate['function']] => [ 'field' => $column]
            ]
        ];
    }

    /**
     * Compile an update statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $values
     * @return string
     */
    public function compileUpdate(Builder $query, $values)
    {
        $sql = [
            'index'=> $query->getConnection()->getDatabaseName(),
            'type'=> $query->from,
            'body'=> ['doc'=>$values]
            // 'id' => $docId
        ];

        return json_encode($params);
    }

     /**
     * Compile the "order by" portions of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $orders
     * @return string
     */
    protected function compileOrders(Builder $query, $orders)
    {
        $sorts = [];
        foreach($orders as $order) {
            $sorts[] = [$order['column'] => $order['direction']];
        }
        return $sorts;
    }

}
