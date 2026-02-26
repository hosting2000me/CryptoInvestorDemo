import { Coins, CircleDollarSign, Gem, Landmark, BadgeDollarSign, Banknote, PiggyBank, HandCoins, WalletMinimal, ShieldCheck } from "lucide-react";

const icons = [
  { name: "1. Coins", Icon: Coins },
  { name: "2. CircleDollarSign", Icon: CircleDollarSign },
  { name: "3. Gem", Icon: Gem },
  { name: "4. Landmark", Icon: Landmark },
  { name: "5. BadgeDollarSign", Icon: BadgeDollarSign },
  { name: "6. Banknote", Icon: Banknote },
  { name: "7. PiggyBank", Icon: PiggyBank },
  { name: "8. HandCoins", Icon: HandCoins },
  { name: "9. WalletMinimal", Icon: WalletMinimal },
  { name: "10. ShieldCheck", Icon: ShieldCheck },
];

const IconPicker = () => (
  <div className="min-h-screen bg-background flex items-center justify-center">
    <div className="grid grid-cols-2 sm:grid-cols-5 gap-6 p-8">
      {icons.map(({ name, Icon }) => (
        <div key={name} className="flex flex-col items-center gap-3 p-6 rounded-xl border bg-card hover:border-primary/50 transition-colors">
          <Icon className="h-10 w-10 text-primary" />
          <span className="text-sm text-muted-foreground font-medium">{name}</span>
        </div>
      ))}
    </div>
  </div>
);

export default IconPicker;
