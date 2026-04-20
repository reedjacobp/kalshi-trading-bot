import { Switch, Route, Router } from "wouter";
import { useHashLocation } from "wouter/use-hash-location";
import { queryClient } from "./lib/queryClient";
import { QueryClientProvider } from "@tanstack/react-query";
import { Toaster } from "@/components/ui/toaster";
import { TooltipProvider } from "@/components/ui/tooltip";
import { Shell } from "@/components/shell";
import { useSSE } from "@/lib/sse";
import DashboardPage from "@/pages/dashboard";
import PnLPage from "@/pages/pnl";
import MarketPage from "@/pages/market";
import RiskPage from "@/pages/risk";
import BacktestPage from "@/pages/backtest";
import NotFound from "@/pages/not-found";

function AppRoutes() {
  const { data, connected } = useSSE();
  return (
    <Switch>
      {/* Dashboard renders the rider design with its own header/footer — bypass the Shell chrome */}
      <Route path="/"><DashboardPage data={data} connected={connected} /></Route>
      <Route>
        <Shell connected={connected} stats={data?.stats}>
          <Switch>
            <Route path="/pnl"><PnLPage data={data} /></Route>
            <Route path="/market"><MarketPage data={data} /></Route>
            <Route path="/risk"><RiskPage data={data} /></Route>
            <Route path="/backtest"><BacktestPage data={data} /></Route>
            <Route><NotFound /></Route>
          </Switch>
        </Shell>
      </Route>
    </Switch>
  );
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <Toaster />
        <Router hook={useHashLocation}>
          <AppRoutes />
        </Router>
      </TooltipProvider>
    </QueryClientProvider>
  );
}

export default App;
