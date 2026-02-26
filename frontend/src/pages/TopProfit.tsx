import Header from "@/components/Header";
import DataTable from "@/components/DataTable";
import profitData from "@/data/topProfit.json";
import { CircleDollarSign } from "lucide-react";

const columns = [
  { key: "address" as const, label: "Address" },
  {
    key: "profit" as const,
    label: "Profit ($)",
    format: (v: unknown) => `$${Number(v).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
  },
];

const TopProfit = () => (
  <div className="min-h-screen bg-background">
    <Header />
    <main className="container py-8">
      <div className="flex items-center gap-3 mb-6">
        <div className="flex items-center justify-center h-10 w-10 rounded-full bg-primary/10 border border-primary/20">
          <CircleDollarSign className="h-5 w-5 text-primary" />
        </div>
        <div>
          <h1 className="text-2xl font-bold">Top Profit $</h1>
          <p className="text-sm text-muted-foreground">Most profitable addresses</p>
        </div>
      </div>
      <DataTable data={profitData} columns={columns} />
    </main>
  </div>
);

export default TopProfit;
