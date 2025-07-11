import logging
import openai
from typing import List, Dict, Optional
import config

logger = logging.getLogger(__name__)

class InsightsGenerator:
    def __init__(self):
        if config.openai_api_key:
            self.enabled = True
        else:
            self.enabled = False
            logger.warning("OpenAI API key not configured, insights disabled")

    async def generate_trade_insights(self, strategy_name: str, trades: List[Dict], currency: str, size: float, premium: float, index_price: float) -> Optional[str]:
        """
        Generate insights for options trading strategy using OpenAI API
        
        Args:
            strategy_name: Name of the trading strategy (e.g., "LONG BTC CALL SPREAD")
            trades: List of trade objects with details
            currency: Currency (BTC/ETH)
            size: Position size
            premium: Net premium paid/received
            index_price: Current underlying price
            
        Returns:
            Insights string (max 100 words) or None if disabled/failed
        """
        if not self.enabled:
            return None
            
        try:
            # Build context for the AI
            context = self._build_trade_context(strategy_name, trades, currency, size, premium, index_price)
            
            # Create prompt for insights
            prompt = f"""You are an options trading expert. Based on the following block trade, generate a concise (≤100 words) market insight for Telegram users.

{context}

Focus on:

Market direction & timing (bullish/bearish/neutral; short/medium/long-term)

Risk/reward setup (defined risk, convexity, premium flow)

Key strikes & expiries (levels that matter)

Volatility view (long/short vol, IV context)

Positioning/sentiment signal (dealer flow, hedging, crowd behavior)

Tone: professional, actionable, to the point. For fast-paced traders.

✅ Do:

Be specific, e.g. “Bullish bias into July expiry” or “Long vol via OTM put spread”

Emphasize trade intent, structure logic, and possible market implications

❌ Do not:

Do not describe the trade structure only (assume the reader has seen it)

Do not give generic commentary (e.g. “Vol is elevated” without context)

Do not speculate without evidence from trade data

Do not use vague timeframes (e.g. “soon”, “in the future”)"""

            client = openai.OpenAI(api_key=config.openai_api_key)
            response = client.chat.completions.create(
                model="gpt-4.1",
                messages=[
                    {"role": "system", "content": "You are an expert options trader providing concise market insights."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=150,
                temperature=0.7
            )
            
            insights = response.choices[0].message.content.strip()
            
            # Ensure it's within word limit
            words = insights.split()
            if len(words) > 100:
                insights = ' '.join(words[:100]) + "..."
                
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate insights: {e}")
            return None

    def _build_trade_context(self, strategy_name: str, trades: List[Dict], currency: str, size: float, premium: float, index_price: float) -> str:
        """Build context string for AI prompt"""
        
        context_parts = [
            f"Strategy: {strategy_name}",
            f"Asset: {currency}",
            f"Current Price: ${index_price:,.2f}",
            f"Position Size: {size}",
            f"Net Premium: {premium:,.4f} {'₿' if currency=='BTC' else 'Ξ'}"
        ]
        
        # Add strike and expiry info if available
        if trades and len(trades) > 0:
            if trades[0].get("strike"):
                strikes = [str(trade.get("strike", "")) for trade in trades if trade.get("strike")]
                if strikes:
                    context_parts.append(f"Strikes: {'/'.join(strikes)}")
                    
            if trades[0].get("expiry"):
                expiries = list(set([trade.get("expiry", "") for trade in trades if trade.get("expiry")]))
                if expiries:
                    context_parts.append(f"Expiry: {'/'.join(expiries)}")
                    
            # Add IV if available
            if trades[0].get("iv"):
                avg_iv = sum([float(trade.get("iv", 0)) for trade in trades if trade.get("iv")]) / len([t for t in trades if t.get("iv")])
                context_parts.append(f"Avg IV: {avg_iv:.1f}%")
        
        return "\n".join(context_parts)

# Global instance
insights_generator = InsightsGenerator()
