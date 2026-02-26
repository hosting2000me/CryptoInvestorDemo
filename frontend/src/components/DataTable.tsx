import { useState, useMemo } from "react";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Table, TableHeader, TableBody, TableRow, TableHead, TableCell,
} from "@/components/ui/table";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ArrowUpDown, ArrowUp, ArrowDown, Search } from "lucide-react";

interface Column<T> {
  key: keyof T;
  label: string;
  format?: (value: T[keyof T]) => string;
}

interface DataTableProps<T extends Record<string, unknown>> {
  data: T[];
  columns: Column<T>[];
  pageSize?: number;
}

type SortDir = "asc" | "desc" | null;

export default function DataTable<T extends Record<string, unknown>>({
  data,
  columns,
  pageSize = 10,
}: DataTableProps<T>) {
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<keyof T | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>(null);
  const [page, setPage] = useState(1);
  const [currentPageSize, setCurrentPageSize] = useState(pageSize);

  const pageSizeOptions = [10, 25, 50, 100];

  const filtered = useMemo(() => {
    if (!search) return data;
    const q = search.toLowerCase();
    return data.filter((row) =>
      columns.some((col) => String(row[col.key]).toLowerCase().includes(q))
    );
  }, [data, search, columns]);

  const sorted = useMemo(() => {
    if (!sortKey || !sortDir) return filtered;
    return [...filtered].sort((a, b) => {
      const va = a[sortKey];
      const vb = b[sortKey];
      if (typeof va === "number" && typeof vb === "number") {
        return sortDir === "asc" ? va - vb : vb - va;
      }
      return sortDir === "asc"
        ? String(va).localeCompare(String(vb))
        : String(vb).localeCompare(String(va));
    });
  }, [filtered, sortKey, sortDir]);

  const totalPages = Math.max(1, Math.ceil(sorted.length / currentPageSize));
  const safePage = Math.min(page, totalPages);
  const paged = sorted.slice((safePage - 1) * currentPageSize, safePage * currentPageSize);

  const toggleSort = (key: keyof T) => {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : d === "desc" ? null : "asc"));
      if (sortDir === "desc") setSortKey(null);
    } else {
      setSortKey(key);
      setSortDir("asc");
    }
    setPage(1);
  };

  const SortIcon = ({ col }: { col: keyof T }) => {
    if (sortKey !== col) return <ArrowUpDown className="h-3.5 w-3.5 text-muted-foreground" />;
    return sortDir === "asc" ? (
      <ArrowUp className="h-3.5 w-3.5 text-primary" />
    ) : (
      <ArrowDown className="h-3.5 w-3.5 text-primary" />
    );
  };

  // pagination range
  const range = useMemo(() => {
    const pages: (number | "...")[] = [];
    if (totalPages <= 7) {
      for (let i = 1; i <= totalPages; i++) pages.push(i);
    } else {
      pages.push(1);
      if (safePage > 3) pages.push("...");
      for (let i = Math.max(2, safePage - 1); i <= Math.min(totalPages - 1, safePage + 1); i++) {
        pages.push(i);
      }
      if (safePage < totalPages - 2) pages.push("...");
      pages.push(totalPages);
    }
    return pages;
  }, [totalPages, safePage]);

  return (
    <div className="space-y-4">
      {/* Search */}
      <div className="relative max-w-sm">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
        <Input
          placeholder="Поиск по адресу..."
          value={search}
          onChange={(e) => { setSearch(e.target.value); setPage(1); }}
          className="pl-9"
        />
      </div>

      {/* Table */}
      <div className="rounded-lg border bg-card">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-12">#</TableHead>
              {columns.map((col) => (
                <TableHead key={String(col.key)}>
                  <button
                    onClick={() => toggleSort(col.key)}
                    className="flex items-center gap-1.5 hover:text-foreground transition-colors"
                  >
                    {col.label}
                    <SortIcon col={col.key} />
                  </button>
                </TableHead>
              ))}
            </TableRow>
          </TableHeader>
          <TableBody>
            {paged.length === 0 ? (
              <TableRow>
                <TableCell colSpan={columns.length + 1} className="text-center text-muted-foreground py-8">
                  Ничего не найдено
                </TableCell>
              </TableRow>
            ) : (
              paged.map((row, i) => (
                <TableRow key={i}>
                  <TableCell className="text-muted-foreground font-mono text-xs">
                    {(safePage - 1) * currentPageSize + i + 1}
                  </TableCell>
                  {columns.map((col) => (
                    <TableCell key={String(col.key)} className="font-mono text-sm">
                      {col.format ? col.format(row[col.key]) : String(row[col.key])}
                    </TableCell>
                  ))}
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div className="flex items-center gap-3">
          <p className="text-sm text-muted-foreground">
            {sorted.length} записей · Страница {safePage} из {totalPages}
          </p>
          <Select
            value={String(currentPageSize)}
            onValueChange={(v) => { setCurrentPageSize(Number(v)); setPage(1); }}
          >
            <SelectTrigger className="w-[120px] h-8 text-sm">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {pageSizeOptions.map((s) => (
                <SelectItem key={s} value={String(s)}>{s} на стр.</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <div className="flex items-center gap-1">
          <Button
            variant="outline" size="sm"
            disabled={safePage <= 1}
            onClick={() => setPage(safePage - 1)}
          >
            Назад
          </Button>
          {range.map((p, idx) =>
            p === "..." ? (
              <span key={`e${idx}`} className="px-2 text-muted-foreground">…</span>
            ) : (
              <Button
                key={p}
                variant={p === safePage ? "default" : "outline"}
                size="sm"
                className="min-w-[36px]"
                onClick={() => setPage(p)}
              >
                {p}
              </Button>
            )
          )}
          <Button
            variant="outline" size="sm"
            disabled={safePage >= totalPages}
            onClick={() => setPage(safePage + 1)}
          >
            Вперёд
          </Button>
        </div>
      </div>
    </div>
  );
}
