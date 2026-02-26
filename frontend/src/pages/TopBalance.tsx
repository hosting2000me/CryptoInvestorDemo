import Header from "@/components/Header";
import DataTable from "@/components/DataTable";
import balanceData from "@/data/topBalance.json";
import { Coins } from "lucide-react";

const columns = [
  { key: "address" as const, label: "Address" },
  {
    key: "balance" as const,
    label: "Balance (BTC)",
    format: (v: unknown) => Number(v).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
  },
];

const TopBalance = () => (
  <div className="min-h-screen bg-background">
    <Header />
    <main className="container py-8">
      <div className="flex items-center gap-3 mb-6">
        <div className="flex items-center justify-center h-10 w-10 rounded-full bg-primary/10 border border-primary/20">
          <Coins className="h-5 w-5 text-primary" />
        </div>
        <div>
          <h1 className="text-2xl font-bold">Top Balance</h1>
          <p className="text-sm text-muted-foreground">Largest wallets by balance</p>
        </div>
      </div>
      <DataTable data={balanceData} columns={columns} />
    </main>
  </div>
);

export default TopBalance;
