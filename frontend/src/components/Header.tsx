import { Coins, ChevronDown } from "lucide-react";
import { useState, useRef, useEffect } from "react";
import { Link } from "react-router-dom";

interface MenuItem {
  label: string;
  href: string;
}

interface MenuGroup {
  label: string;
  items: MenuItem[];
}

const menuGroups: MenuGroup[] = [
  {
    label: "Addresses",
    items: [
      { label: "Top Balance", href: "/addresses/top-balance" },
      { label: "Top Profit $", href: "/addresses/top-profit" },
      { label: "Equity", href: "/addresses/equity" },
    ],
  },
];

const DropdownMenu = ({ group }: { group: MenuGroup }) => {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-1 px-4 py-2 text-sm font-medium text-foreground hover:text-primary transition-colors rounded-md hover:bg-accent"
      >
        {group.label}
        <ChevronDown className={`h-4 w-4 transition-transform ${open ? "rotate-180" : ""}`} />
      </button>
      {open && (
        <div className="absolute top-full left-0 mt-1 min-w-[180px] rounded-lg border bg-popover p-1 shadow-lg z-50 animate-in fade-in-0 zoom-in-95">
          {group.items.map((item) => (
            <Link
              key={item.href}
              to={item.href}
              onClick={() => setOpen(false)}
              className="block rounded-md px-3 py-2 text-sm text-popover-foreground hover:bg-accent hover:text-accent-foreground transition-colors"
            >
              {item.label}
            </Link>
          ))}
        </div>
      )}
    </div>
  );
};

const Header = () => {
  return (
    <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container flex h-14 items-center">
        <Link to="/" className="flex items-center gap-2 mr-8">
          <div className="flex items-center justify-center h-8 w-8 rounded-full bg-primary text-primary-foreground">
            <Coins className="h-5 w-5" />
          </div>
          <span className="text-lg font-bold text-gradient">CryptoInvestor</span>
        </Link>

        <nav className="flex items-center gap-1">
          {menuGroups.map((group) => (
            <DropdownMenu key={group.label} group={group} />
          ))}
        </nav>
      </div>
    </header>
  );
};

export default Header;
