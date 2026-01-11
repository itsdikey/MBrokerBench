using System.Text;

namespace MSodaClient;

public enum SoqlOrderDirection
{
    ASC,
    DESC
}

public class SoqlQuery
{
    public static readonly string Delimiter = ",";

    public static readonly string SelectKey = "$select";

    public static readonly string WhereKey = "$where";

    public static readonly string OrderKey = "$order";

    public static readonly string GroupKey = "$group";

    public static readonly string HavingKey = "$having";

    public static readonly string LimitKey = "$limit";

    public static readonly string OffsetKey = "$offset";

    public static readonly string SearchKey = "$q";

    public static readonly string QueryKey = "$query";

    [Obsolete("Socrata provides $select = * by default, so this is field is no longer needed and will be removed in the next release.")]
    public static readonly string[] DefaultSelect = new string[1] { "*" };

    public static readonly SoqlOrderDirection DefaultOrderDirection = SoqlOrderDirection.ASC;

    public static readonly string[] DefaultOrder = new string[1] { ":id" };

    [Obsolete("The maximum limit has been removed for Socrata 2.1 endopoints. This field will be removed in a future release.")]
    public static readonly int MaximumLimit = 50000;

    public string[] SelectColumns { get; private set; }

    public string[] SelectColumnAliases { get; private set; }

    public string WhereClause { get; private set; }

    public SoqlOrderDirection OrderDirection { get; private set; }

    public string[] OrderByColumns { get; private set; }

    public string[] GroupByColumns { get; private set; }

    public string HavingClause { get; private set; }

    public int LimitValue { get; private set; }

    public int OffsetValue { get; private set; }

    public string SearchText { get; private set; }

    public string RawQuery { get; private set; }

    public SoqlQuery()
    {
        SelectColumns = new string[0];
        SelectColumnAliases = new string[0];
        OrderDirection = DefaultOrderDirection;
    }

    public SoqlQuery(string query)
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            throw new ArgumentOutOfRangeException("query", "A SoQL query is required");
        }

        RawQuery = query;
    }

    public override string ToString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        if (!string.IsNullOrEmpty(RawQuery))
        {
            stringBuilder.AppendFormat("{0}={1}", QueryKey, RawQuery);
        }
        else
        {
            if (SelectColumns.Length != 0)
            {
                stringBuilder.AppendFormat("{0}=", SelectKey);
                List<string> list = SelectColumns.Zip(SelectColumnAliases, (string c, string a) => $"{c} AS {a}").ToList();
                if (SelectColumns.Length > SelectColumnAliases.Length)
                {
                    list.AddRange(SelectColumns.Skip(SelectColumnAliases.Length));
                }

                stringBuilder.Append(string.Join(Delimiter, list));
            }

            if (OrderByColumns != null)
            {
                stringBuilder.AppendFormat("&{0}={1} {2}", OrderKey, string.Join(Delimiter, OrderByColumns), OrderDirection);
            }

            if (!string.IsNullOrEmpty(WhereClause))
            {
                stringBuilder.AppendFormat("&{0}={1}", WhereKey, WhereClause);
            }

            if (GroupByColumns != null && GroupByColumns.Any())
            {
                stringBuilder.AppendFormat("&{0}={1}", GroupKey, string.Join(Delimiter, GroupByColumns));
            }

            if (!string.IsNullOrEmpty(HavingClause))
            {
                stringBuilder.AppendFormat("&{0}={1}", HavingKey, HavingClause);
            }

            if (OffsetValue > 0)
            {
                stringBuilder.AppendFormat("&{0}={1}", OffsetKey, OffsetValue);
            }

            if (LimitValue > 0)
            {
                stringBuilder.AppendFormat("&{0}={1}", LimitKey, LimitValue);
            }

            if (!string.IsNullOrEmpty(SearchText))
            {
                stringBuilder.AppendFormat("&{0}={1}", SearchKey, SearchText);
            }
        }

        return stringBuilder.ToString();
    }

    public string ToString(bool forPost)
    {
        if(!forPost)
        {
            return ToString();
        }
        // If a RawQuery was already provided, use it.
        if (!string.IsNullOrEmpty(RawQuery))
        {
            return $"{RawQuery}";
        }

        StringBuilder queryBuilder = new StringBuilder();

        // 1. SELECT
        queryBuilder.Append("SELECT ");
        if (SelectColumns.Length != 0)
        {
            var selectParts = SelectColumns.Zip(SelectColumnAliases.Concat(Enumerable.Repeat("", SelectColumns.Length)),
                (c, a) => string.IsNullOrEmpty(a) ? c : $"{c} AS {a}");
            queryBuilder.Append(string.Join(Delimiter, selectParts));
        }
        else
        {
            queryBuilder.Append("*");
        }

        // 2. WHERE
        if (!string.IsNullOrEmpty(WhereClause))
        {
            queryBuilder.AppendFormat(" WHERE {0}", WhereClause);
        }

        // 3. GROUP BY
        if (GroupByColumns != null && GroupByColumns.Any())
        {
            queryBuilder.AppendFormat(" GROUP BY {0}", string.Join(Delimiter, GroupByColumns));
        }

        // 4. HAVING
        if (!string.IsNullOrEmpty(HavingClause))
        {
            queryBuilder.AppendFormat(" HAVING {0}", HavingClause);
        }

        // 5. ORDER BY
        if (OrderByColumns != null && OrderByColumns.Any())
        {
            queryBuilder.AppendFormat(" ORDER BY {0} {1}", string.Join(Delimiter, OrderByColumns), OrderDirection);
        }

        // 6. LIMIT / OFFSET
        if (LimitValue > 0) queryBuilder.AppendFormat(" LIMIT {0}", LimitValue);
        if (OffsetValue > 0) queryBuilder.AppendFormat(" OFFSET {0}", OffsetValue);

        // Final result wrapped in the $query parameter
        return $"{queryBuilder.ToString()}";
    }

    public SoqlQuery Select(params string[] columns)
    {
        SelectColumns = getNonEmptyValues(columns) ?? new string[0];
        return this;
    }

    public SoqlQuery As(params string[] columnAliases)
    {
        SelectColumnAliases = (getNonEmptyValues(columnAliases) ?? new string[0]).Select((string a) => a.ToLower()).ToArray();
        return this;
    }

    public SoqlQuery Where(string predicate)
    {
        WhereClause = predicate;
        return this;
    }

    public SoqlQuery Where(string format, params object[] args)
    {
        return Where(string.Format(format, args));
    }

    public SoqlQuery Order(params string[] columns)
    {
        return Order(DefaultOrderDirection, columns);
    }

    public SoqlQuery Order(SoqlOrderDirection direction, params string[] columns)
    {
        OrderDirection = direction;
        OrderByColumns = getNonEmptyValues(columns) ?? DefaultOrder;
        return this;
    }

    public SoqlQuery Group(params string[] columns)
    {
        GroupByColumns = getNonEmptyValues(columns);
        return this;
    }

    public SoqlQuery Having(string predicate)
    {
        HavingClause = predicate;
        return this;
    }

    public SoqlQuery Having(string format, params object[] args)
    {
        return Having(string.Format(format, args));
    }

    public SoqlQuery Limit(int limit)
    {
        if (limit <= 0)
        {
            throw new ArgumentOutOfRangeException("limit");
        }

        LimitValue = Math.Min(limit, MaximumLimit);
        return this;
    }

    public SoqlQuery Offset(int offset)
    {
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException("offset");
        }

        OffsetValue = offset;
        return this;
    }

    public SoqlQuery FullTextSearch(string searchText)
    {
        SearchText = searchText;
        return this;
    }

    public SoqlQuery FullTextSearch(string format, params object[] args)
    {
        return FullTextSearch(string.Format(format, args));
    }

    private static string[] getNonEmptyValues(IEnumerable<string> source)
    {
        if (source != null && source.Any((string s) => !string.IsNullOrEmpty(s)))
        {
            return source.Where((string s) => !string.IsNullOrEmpty(s)).ToArray();
        }

        return null;
    }
}