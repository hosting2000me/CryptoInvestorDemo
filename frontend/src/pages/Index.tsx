import Header from "@/components/Header";
import { CircleDollarSign, LineChart, Coins } from "lucide-react";

const Index = () => {
  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="container py-16">
        <div className="flex flex-col items-center text-center gap-6 max-w-2xl mx-auto">
          <div className="flex items-center justify-center h-16 w-16 rounded-full bg-primary/10 border border-primary/20">
            <Coins className="h-8 w-8 text-primary" />
          </div>
          <h1 className="text-4xl font-bold tracking-tight">
            <span className="text-gradient">CryptoInvestor</span>
          </h1>
          <p className="text-lg text-muted-foreground">
            Crypto address analytics — balance, profit & equity in one place.
          </p>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 w-full mt-8">
            {[
              { icon: Coins, title: "Top Balance", desc: "Largest wallets" },
              { icon: CircleDollarSign, title: "Top Profit $", desc: "Most profitable" },
              { icon: LineChart, title: "Equity", desc: "Equity analysis" },
            ].map((card) => (
              <div
                key={card.title}
                className="rounded-xl border bg-card p-6 text-card-foreground shadow-sm hover:shadow-md transition-shadow hover:border-primary/30"
              >
                <card.icon className="h-6 w-6 text-primary mb-3" />
                <h3 className="font-semibold text-sm">{card.title}</h3>
                <p className="text-xs text-muted-foreground mt-1">{card.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </main>
    </div>
  );
};

export default Index;
