# Breakout confirm study, 2026-11-18

The set is twenty constant-product paths. Eight climb from 30 virtual SOL to 110. Six fade without leaving the launch. Six spike to 40 virtual SOL and give it back. PumpPortal does not serve history, and the pump.fun frontend API returns Cloudflare 403 from this network, so these paths are reconstructions, not a live archive. Training is every path except the last climb, the last fade, and the last two spikes. Those four are the holdout.

Size is 200000000 lamports, about 0.2 SOL. Fees are the paper venue fee of 100 bps plus the network cost. Exits are the desk defaults: stop 30, take-profit 120, trailing 35, max hold 3600. The 10x floor is applied. The deployer line is not in this set and is not changed.

The training search picked the band 1.40 to 2.50 times the launch virtual SOL.

Holdout, that band: 1 trade, hit rate 10000 bps, average PnL 501558372 lamports, worst cumulative drawdown 0.

Holdout, buying the launch band 1.00 to 1.05: 4 trades, hit rate 2500 bps, average PnL 44776114 lamports, worst cumulative drawdown -267628302 lamports.

The confirmation band skips the fades and the spikes. It is enabled only with `[strategies] breakout_confirm = true`. A KILL still ends the mint.

The 3-signature deployer line is unchanged. This set has no deployer signatures, so it does not say how many of the skipped climbs would have died on that line. Live audits on new creates still apply it.
