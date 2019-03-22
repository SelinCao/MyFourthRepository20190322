package Tools;


import org.apache.commons.codec.binary.Base64;

public class FeatureCompUtil {

    /**
     * As an example
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        FeatureCompUtil fc = new FeatureCompUtil();

        // similarity should be 0.00466856;
        // String f1 =
        // "VU4AAAAAAAAMAgAALr/vPRVuwTuECL07hZ5tPPBTeb1mdnw9DTwDvgZjSr0PzhQ+uNXCvSYrFL09PF09pokrvKqnzb1uXY89hWO0PafOR70A93c92wfDPQlgzb2SClg8QExePuIA8b3SBhc+mLJfPRT4vbwRvRQ9ryqQPVqW1L0dLFM+SL6zPSjm+TxHNBY6jw1kvjZuvTwtaL28sZVIvcpQ6jxXJiI9qVy3vSVgdTvFiRO9RHEWvnX94jqjcI+9+hjYPRmDlL38lRs9M2vtPUzx4727nB89R19MPfbSsb3WwWi9nh7avJfI9D0LBn6+H5eMPTaqAz7mHbU9QKhaPQO8HD6Tmki+tXsEPdCfRr1nOo299gfnvb3TaT1HNlc+tnprvQO/cb1qwhS+Us/yPYEPCjwxnXY8/1+5PUbkrT3fCCC+rrVIPAds7LwbvG48VOCsPbfqAz6zJKu9udbkPdpwCr2zfEA95hhsvfudVDsRfgM+N3NAPf5wWbyqcCa867w1vVIO6Ty7nLA90f2ZvZaCHj2I66k83Xd0vaZLGb0Thse6vvKyvOrf0jwgzgE9XJZJPfbj87w9Bhg9iewrPmtqHD28bqY8Z3xpPRcfZzusy7q8K7GfPZ8o8jvs39+9fNUtPfM0cL0aj9k92J/vvWAd6T0YWUe+WIQKvktStLxiAAS8OiEJPCBS2Tw=";
        // String f2 =
        // "VU4AAAAAAAAMAgAAd6r4vWKkUzzcaXO9zoECPc5Ghr2ca2e+UKkivm/1vrwdyoG9RW0JPhuUxb0nIIc8WZOkPGGP6ryJgog8zI1AvR1gHz1E7ak9rx+TPWLfLr4RXNi9eGpYPR+0zb3dgrE8CFb3PcHGsb0i+yi8xw+mveyK1TsUSpM7oAR9vbpsybzxZWW98jwyvUuW3T392Rm++Cv3PR3H0z3Sf408Nd6/vJtFyj00aRw+TbkWPhvEJr78wg+8CHkaPXOQEz4q5XQ9JL6dPWV+mr2pSoy9cyRhPBBWGr0FiQ4+sRykPTKyzj1Z/ms+8CnPvTHQwr25oMO9ETz9vHPDZj6MohC+b2m4vGMppT3R1p29wY3lPYIE3b0/T5y9Q8SGvE72U70GP0s9QVHQPATkDD59koE7WuUWvuZFBTwBavE9zJc3vXUrKb4t+JU9Yeo/PSHPjj3Fhy69A1SEvWz4ED6/UZ08Y4ddPNK24bzBVaS9yTj6PTkikr0ExO09rH9EPEBGqT3E4+A8txmDvdk/K70m6cS9dwavPHZtRbzFh8k8phOSvfA2sD2kNze8gH6WvfCNMr1FOtM6hH6KuymG7D2kueM8jVLEveD10D2+xzW9xQQjPccpmr358Mo4LleQvSyeXbyta4I7uHmau3Ygn72cqRu+VYMsvnIDvT0PBO08Ab2rPVFolD0=";

        // similarity should be 0.341673;
        // String f1 =
        // "7qpXQtViAACAAAAAPdyRPaXIGr5Zkv49GO9xPiidST39ruI9gW8UvXJnar7wdwo+cQtEPN6US7351A48vzPTvCMkZL3yAZW9ZDS9PR9Udj1SHgS96pWnvOpv5rvm1sC9/0oWPV4YEzvqvsg9kIOdvLLWP73VJ9Q8CR0dO2dfsr01UZ08tV6QvDj3mb1ij7g9rOQxPVbfOb4wcns8Mn5yPhhhxD0h/rs9zfL1vb8g7z0IzwC+OC2RvZnN7L0/YXU9DSqxPdBNCT16DpK96LbhOq3ztDuzyJ089asBvJBYSL3lGK09GCUFvqiXpD1hy5+9gExGvQMalz0WyBU9bGbdPdmJrz26csG9dfqgPcPelb0kyMW9NSKhvQpn7TzSWzA871N4vcuxfD1wnL29dgZmPRbBGT1zLg0+uZ7QvSPyB77dd5A9pKsSPQGmmb1h+mG9j+y1uvVeAT4Qh4g6jg1ivoF2Uj1AEbm9fKdavWoQb7wdLwA97sHvPW2SNL1JG+w8VKeWvScQZL2eoeG8kbzXvNtWn71GLVU9Iiq8vXHfuz1YL7k9yU4KPsPEBT5KQYq9rR8HvfEosz2wnbC8cG2+vUDk1j3Wyp699DLlvVd4lz2Tfoc8WzQePshexzy8/t+9aMimPLq31b3X2Ko8R+hgumhs/z1g04E95uZ6vhehGz0Go8k85fxxPUKLOz0=";
        // String f2 =
        // "7qpXQtViAACAAAAAxStDvrwopT1PxDk+xL5cOXL0Qr1uej29lKbpPXe7C70/h9W89i+nOqn1Ez3/ODi96+oUvXZNu72td/s8pOeZveYpljxo2lM8otIOPUzi8T3B94q7xvMLu5Ga7D2ZDT4+vFqJvZmTxDy0Db68d7UdvbG2Xz3CM4Y80NJ/PRMsNz3HKYi9rELBPANjDL3tHg89dYIqvWp5yL2zMUS9l5L0PNiFszzr9N08AAXDPRyiCL6BqxE9vxibPGIbg73nWtE8u24EvSldm710ude8tT9yPfTzZjyI9om9UJrtPfoDIb0zRv29+D+WvTTkLz6Ovxk9zOWAvUejiDy2qdg8Tz2zPS/xyT2GoGQ8DL+hPGbOqr2HZXm7qsf+va65473llzG9MByNvYnv+D09WjC9TgROPbnofTxrsEw+4GITPJ771j3iIKg8fJGlvWrUczyDeMe8TWvQvXWfA7yC9sq9VoblPGPFhT17oti8i7U1PRr8AD2uGgu+RxEOvlJNkL6D5xu+TdGGvZlVcj5LY1q9mKzGPFD+gz3Ohio+HXHbuy+ntjzSusQ9UOBaPiiOWz0Z5TW+p0/9vJLDRT57/ta9DGYIvbFf2rs1PAo+vhEEPg9+872BmsW9I11tPf7AFD3SRJu7M7RhPHmz9DvKnps9AJ0Fvhlc/L0rDr89iLKZPApLwb0=";

        String fea1 = "7qpXQsFdAAAAAQAAYrmZvEpTrj1JDu68WFr4vJCggT0mCLA9kE0ZvQP3ljy1Brw6pvmtvV8eub1CLJa8LKliPUHB+Dy2o3i7T0IJPq0zn72rDNw8mbCDvVF3qLrm8eQ7WCzYPaAoFL51gQe9NAVKO3RvbL2Pjg+9gdemPY1PQry7kS+9mlqmvSoGXD0bbb88eWzcPF60Hb1zjEY7haCQPYgKlz1jGnY8DmuBvQHrED3462w8DTBfPZJdMj1WdXU9s5ScPTpGu70mbye9AmyPvRVyjL26Evs4EHanN8gWAT2+qPq9GMJ6vYUzij2mDoS9GL7gvSF+r7xgmbk9x0sOunDHMD2j60i9FvYFPbqh2rzfrZM9cuKJPZtOyDr7+QG+fxCdvAthNLyMZgU60cf1vUH4a70TrPW82cJWPURQKb2c9Mw8UCRavLDetbufRva87r2YvexTib3non69tUWJvaJK+jzBWdU83AtivXrtrr3ys4u8l/yTPbNhkb2nOCg9K0kAPZrWAj1IsRY9DavDOjwZBz6grTE966gTPkIXWD0ig349faggvJuoRz2hTjq+xpw9PXZxGDsmV6g97+GjvMToVb2HYoc9eJ2VPYQWIr2azQy8d95IPe+hYbzaray83ai6PDbx5jwPrLo9kb8gPau/ILwWgYS9bVsdvefoG73ltKk8CDBFOhFcCr09KGM9EUrqPTVU/buBsw481wzRvIWkpLwDune9K6GhuiGW4jkoSzc9EP5tvecD0D2jma09tJdQvNbJtbsgxOC8F2rxvMk3iTy3it+8AP5hvY4aXb0dHeA8to+AvUBAjLyFJzK8WgQRvLeyKj0obp+9mr8nvYbnFb0D9Jg8JXmtvMaIizwJm4S9kdMIvuxo4zylegE85RhEPSyrcL24UDY9CZETvWsONb3FhgW9Hx0ZPLSfCr6/s648cSeTvDt25bxOCZK9VLoLvVkwy7u0tqa8p2uzPdYdZj1gnpO9G/8rPcdn/7wNQAq9ZIWLvUQ/sL3eHKI9XYEAPqJcUjxfXog96AaaPQfyNDvcwIm91/ELvKPcAz3ZgBS9PWPdPL19mr0v+g09hm0DPhr9or3sB5W9CHkGvaoihb3kNdQ95vKlvU/7vb0VKJC9VeI5PB8dmj3kmUC9Wp4pvG7mc7w+uRE+s6+tvBZFw73OpPw7AVmtPdzas72UUri8U5YFPVkjsbwEbOS8C5CLPfwNY71ue+e8vGUQvtNtTD3R/Ty94qPYPcdDe71taVm98Y89PKhge7217wg9MdfePUpjvz1EXMe9IiQyvOj487zYspW9pI2Fvaa1rb2DZkk83I8zvbTgfr57tVU9hdP6vAzIDD7zkSE9nZ8dPTOml7tYRZa9VHPPPQ==";
        String fea2 = "7qpXQsFdAAAAAQAAf4v4PCQPjb1yswM8HUQYvbBijjzwEbE9ayyAPZh5A70USeu82gzDvRCKyb3c6xq9VBviPKUrD73YRoY8Y0W8PIjUwr0MYkS82ptSvVRxmrxiPyS9K2uqPRyjJLwuQBM8ddWBvQK0Zr19mim9sOqpOvmQpLwVkz+8oxuNvfgmoTuDW7E9ijjbvBiKTD0Ki529pjm7PV9+w7xOog69mW3WvGHcN7wB5b+9DQVwPeSa6zwHPIw9mCOxPY4/mr2g/bo9Be9wvJ6BEb3T0Ge9FmFwPTKOBz2tYdM8TVrQO99/AzzI2sm8chqwvfzk5r072J89pRlePSPBCr3zm4k98/9bvTS2HDwRuiG9/FSjPZpbSb1OMR+9BPylvVknGj36te+8x8cgvlS3HLwaDxa9sYjZPAAxM724H3k9Ou6AvYdvEDy2CbG9USNTvVVrg72avHe9g1fWvZa7OLyp3b89nhZIPT9Hw725fh88/8kCPrXuZLxef/k7HwfFvRCmjjuU7qc9WSSevS+nsz3lJYo8NL1wPQwD5z1zjYg8KaPzPFKpDzxyU8e9V9hCPfh4jTwkNwU+B9G9vY0jbL0D3me9QqMAvZMMQbytqYG99xDEPf3zqj2Fp3K8h9n+PFBbEL3E1ka9PFC0PTnTJL1z40C9Po2pPTaL/70khFu9igVTvBKE+TzmUfe4BC2NPczWF7vwP3u99XrEPVCSE70El5M8ZaKVvXuNMDrReqw900NrvVjD3z2wId89wQmuPc1F2jyyYWW92lRhvaU7kzu666C9CpTfO5lTkLwC/kQ9pYGnvLt7Vr1Huw89N7EqPie9HD1dNNO7KreJPc+l9Txbwja9yyW+PNAglzwgc/K9xUapvSfs6TyA5ZG9AiEjPYzyLr0MrYw9sPqmuaAKYr34fL49okhaPEZ6djz8BIg9r1g9OyEgZzz7YlI9bTXUvfKvBryFCtY84lu4PfF53jw2Qdm9bJSDvTkC972r0QW+aKIuvcG7UT0zO4E9NQCjPaWxID3GjW09nyvSPblBhT3RP1m9ep5fu+kdEbxQO1s8EAGWPdjSc72jqSW9gEFtPbkFErwRYl29kZIIPFhns73UB+A920RAPYxrur0LVwq+3EMDvrNcYTtseZ+8xJefPbfAC732wSs9mOS2PC0VMz2ICJM9pC+svOvz4LwV5EI9oZInPZictjzOzIU9iOFiPTy17rxav1G9y7hRPHX7pjyecSu8ZwSHPaTB772hXCA8L1GpvSZiJb2MHhE8hb5UveT9+D1ov7K83GlKPX5JBL6Lr469lLC0O9hUM722mqA9ZayLvcJQNr4Fw4c9UMqKvN4GBT3nZfA80A2vO/fEFr1FfgA9hdOoPQ==";

        //String fea2 = "7qpXQpFlAACAAAAAy+ZavY+/dT2sUPG9UpnbvESzmb1t4aC9jh1Yvvmu7zxTQI899dXYvVQGxz2WFRS89cElPebCj7yRzsw9cI/DvUxNpD39KDU91FFXO5oYmT3GdS49qGw5PcleE7z8vu48LjOfPXF61T0o2FC9v4Q+O6U2jL0luQ++B2vHPXsRnLttUBg+eJyjPWv4C74nJCi9yHQpPQBeBT2IFUm+BRfkvG5cp7182lg9AXRqvdXoDz43XbY9TuMPvqEBjb4kbwq7Ky0svapvQj1/6H09E9FxPX4Hx70Y4I48xrC6vdzv5T3u29S9jFCPvbdE6LyVgWs+FvSIPHc71T3E4pu7FrVTPVJilr2SDOM9uMDcPLQErr0a5iK8dG+UvA59Cj5lrAo9shnqPLdpCj5+Eko9/FeWPV7yxbyceXS8sE8ZPd4TyL2UCBe+2YgWvlFLqjxK57U9vAoRuzb8xjyhXS8+ROuEPDEyq73jmug9DYqzvAhZP73E6fQ9tqEMPebhNL3aptE8kH6Gu62YML4DoMO8CdnHvY0pDT0LPZ49nJsrPdL2FL24V+e8wOQcvIT5Fr1B0SA+QWydvbDZ2rxWs+I8sTEQPXCJkj11mZY9zyMdPadpV70Tlz49XGVmvAC/Mb4B1Y29UbkEvpyrTr0YeeW83dhxO4HEDT56r2e+kVMAvluBBTw=";
        String fea3 = "7qpXQpFlAACAAAAAVPQtPaNWOL4f+Rw8LwnHvY5N+z1KSJ89kRN3PVpCS76234i9wmsVPcHbJL2Ge5e9k/F6PUlmGr643xE9QCFCvVs8Bb7EWby9E5YEPgf/Azxm+dq6aiTCvT/jGr4lIR4923xpPQ5jvL1Ituu9mAfnPB2DHz3CX/A8KauHPAWwwD20xeE7Q6IJPbK+Orxzm8+7WpMbvXv3LjuUos67X8L2PLszfr1RHSK+n7VzPSmVA74pdPQ9qh9/PfyrIb35lzw9KnEoPXwwTz74QgS+qKJdPdgT5LtQ9IS9HLcRvlKVAD6wSAG9c6cCvnJ6a703kDm+tRqNPZjmlTwEaSy+kYnoPZqSUT2eedE91fCqvYNKLL0ACCm8aQeRvMIkj716qkK9og6hvfziNz6AGV2+66Ygva9zDD5HkAc9lmhtPQlTB77P+xU9kluuO0ZvgbwMDxc+HpmfPY9bzjydudg9lzNJPeArVr24Neo8jKTPPZDU5L1s9II9/Ds8PeYkyT3kXfa93WvhPP4vBr50ZKw9ZDzjvY3hCb3F06e6MTB7vdY06jybVIo94lBGPQR8nT1CnLE9hJQYPuznjD2uUnk9gr0ovd7TS71SO9w8LoRcPW0zhjyW30S9DpY9vkllR70fMNC8NcWJPeYL5z2Knxu9s6xWvf8r3jx9zeU8XFQAvDY9Rz4=";
        String fea4 = "7qpXQpFlAACAAAAABCODPUH6U74Jx9i90BLCPBWsbT3ttlu8OLPTPK6zSL62YZC8Wc6wuSQ3Ejx/eJm9H0+hPdzCRL2iq/U9hRFUvSe2sr1Tezy92bb+PbfJozyV9TS9CujLuw3ygr3Iav8963xxvBxeYr1qRNq9gKqAPcqjnL0Gaqw8z2ENPY38mz0cIFA9/MS4vSaQ3LxPz389CqyoOt+pwj1shLq9Fh2JvVdSSLyrKP69r01Lu284o72yxO88Og2UPSe3qr0B4s88PUb3vMSKLj5hNhS+d5ZRPlcG9Ty+1aK9GykQvodpQT42Y4K99hwYvr56Ur1z3xi+/q+kPXiqCD2Uoi6+Qn5OPdWb6r2boKA9r1uPOxNoRr2RNe+8F9Lxvb2Csb0UED++DXV+vQt49zwYazS+HB9JvbE16T1YUw091DsJPneJzb0V8aU9YU0LvrQ8971F71w9At6MO8UkLr0M2j0+b+bZPC/zYr2wQZY9lseGPXTCM72/rRA+UXLHPOmV5Dw7IrC9qLFQPXoiwb1sYok92YLpvcOSgT2werG9197rO3kulT1Nmd27s5YjPhIurD1Q9uA9MCYzPWz6Jb3m3QS6Ln6FvfW8I72vgm+9ovsuPKSOF70exY+9dqqKveMcJj2rVBS9LSMDO3WLjz3qN8E9PTIOvi/aqb2E7le9XpbCvflYCj4=";
        String fea5 = "7qpXQpFlAACAAAAAonUBPZi00rx4ha+8N9lSvcNkHb4d4qY9RnXkvVYkwz0ZZfc9JjIlvAUOSz676z+8f7CvvQJRwb1t4G88HrjBPfG5Ez2r57C8g84NPRqNHr60jSo+8PWzOxp9iT3ciqw9LdAavBQHKr0S6Iq9ef9XvR0eHr1QAYg8ij57PGMjZj33FDM+GwygvdVmGb67SKc75F2MvPijxTxyi4e8IeyJPAteJzzAw908yAUhPitxjj2KHum9wYZLvvspsL3sUkm9jTDtPKaPqj14/Ty+nv/2Pbx+gL1Zh8c9GnqNvf9gEz2qQQ69MaQJvTbgdT1N5T29Crd4vetgO73efwG+FUp0O5fciL0GiVw9/5lWvsopgT2Y9vW9xMUBvhBBLj1bqVu8wsQnvvZTKj1Jt5k8S0PPPObKLz054Qo9J9ugvXzfbr5+T4Y9i/pkva7Clj0j56m9rxBZPfk3771FAvS70QDivWGcA754PBe9Q+bWPa2JTD2b28I9LyZwPSBGp7yWEQg8OImrPd26k7x+cI89KZ5EPb+tKz3Lff892JQFPvxXzryfHrE8g6iOPTfj3z33Ybu8DzlivRcbuTx8xJO9XSKVPUnC5b26hRg+goRAPUhH6jzUnmc8awDduwaKb7zGzao9XMIivTvx9L0tzgQ+oul2vRIXOr7/jU29lNNDPUxnZL4=";
        String fea6 = "7qpXQpFlAACAAAAAZduTPKAg+b1Q91C9Ey+SvQZ3vD2pspM9XnR4PTEPOr5s94u9ttM5vUGVgjzafJM8T+cgPflwZL364Ie7WJPnvJetvL0wBM+90puoPbZbK72f4Ky8NsYavno9bb5itxM9u5FlPXfiZTyVLdm9fP6iPd4Cw7yKxD29HJpXvU5cbj1Ue2Q8lqomvTnLXD3ASRu9BiTlvLxgn7wq1j692yWYuNA1LDqo3Q++UTKFvT08DL7r/Ew+0voTPeSJxr0cQyw9fjiePf2hQT658P+9A36UPfczsT1qiGM6J4RtvrUCBj4L1HC9n4wMvnv2dL2lYaK9hfQOPYuwGj0ZwEi+luUwPWJGPr0X2ys+b0LlPPAF173/4om9A/bZvJk8EL1yXj+9VZdjvVYuZj3SpYS+nn+5vTfLqD3Sqlk9uPLXPcEqzL08poE8ciISvSy7Ar0Pu0o9jlO0PQzJhLzDsxo+7FybOk1vvL2e7He9cxOsPEEEHb4Yiu89DViWvOGPlT3jjB++TibIPSVt3L0FtPk8Sm/IvP/Gaz0LCKS8t5SYvXz5zT3/Qqu8T8GLvOjhMj2Hf0s9+GNSPd/YG7yHKhG9SmgfPX0CpLoLjeC8DrtNvThbFr7BOVc9mwZ7vQH/WLqE8ru9OSgLvMPhnz1BTa69KGPDvQ5OP75MRrg9quYpvTViDz4=";
        String fea7 = "7qpXQpFlAACAAAAAeTYHPhBU0L0lrp69lrLzu3BuLb7ku+27cLoJvmNZIj68hiY9ZVxlvfDsbj4iQjO+ZrQWPU2bMLx/u2Q9pBqzPZF2tj1Tb6q8kk3oPaI8kL3AtDI+5qnkuxRFCj5RfUy8yRq+PCpanT3NRDS9jsH/uyJD/jyPHaY80nOuvM1y2j0zzlA91IylvTLqUr0dSyE9Sno3PvByOLzwsFO9bEQNvVFUvTwp2bc7DdOyPVdmVT0j1uO87i+Pvcguyr2MmG28xTrePUNAeDz4ZAy+qNInPnv3k70VADi8wKlHvrBU7j3hxzi8Tl8YvV+Nazvxg489x//kOqk9jzzpu469gdL5PLXpYDxa0pA9XckLvg9n3L0nWLi9WiY3vn+dcD1hlwm+va7dvZXOar0iVm69N7uHPbnG7TybZ5c92i1YvQigBL6jeAs+0fjrvUtuGz3NHrQ7YKC6Pf+sjztg7fU9BQInvYNAqr2gWMu9sc9BvLd+TL1fapo9naorvWqWnL2E6IE9KFaHPXK+jrrCQAs9EBaUOTowFD7HHR4+YCziPEgXlr3pehU9PlkSvACih71vbFM8+/DzvcAcRz2M0PS8oB+gvTD2zT0e9FQ+2XYKPXZHWD16JIg9O9OjPauhpb0rLZ+9kKYMvKLSi70XTk07G1H7vWQhd758GiC8NH8+vYGwlL0=";
        String fea8 = "7qpXQpFlAACAAAAArCNDPBSJNr5XZ5i7btK1vcAl4z1qPQs9tXsBPYJ8JL4+Sd290E3VPXjDOzyBq8y8g/GJPcaQF748NH09znC5vPeT1720sVY9mRD3PZNn3LxHgyi8yKeQvS7u9L2YWR09WAuNujOptL0cDMm9FZx/PUEjq7reqSC88HTvPJI93z1zw349Zu8dvevxw7yoccg8fP4fvYNzPLxVrmm95asnvbtShL1byyO+YCVSPS9Hzb0HDMA98hWwPb2Qfb3tGCE9BzNCPKd2LT50Zw2+1QmlPCVZzz2lV3O9v5xCvi+xoz28Zfo8yrsrvsUtL7zi2TS+RmjdPSAmQjyNbSm+cJTRPZctsrxbGiI9B9mSvI0KX73IDfy8iRqmvWGn9L1w/7m8H0OZvMmsFz4FjWG+G01vu49eCD59ugc+QX5uvDSZx7zvdg0+8IywvQ58Aj2nug4+USzfPY0lKL1kYeA9Hm4ePZiaR72sz3c9ALOwPcTiPL5LzDM9EPDYPLU7BT0YS1+9/5+gPOijmL0r7cI8FAcAvnfp97yDCd68keRIvbWIC7xHrKI9chrJvNdkrD3SSBA+b/8aPhdXHT1QVsA9gkYkvWeSqz3EFAI8jeqSPNaaLT1cjc29xMBLvpa+hTz25C29+AIuO6+zsj1p9Ug95iofviLtljrRLNS6LaYcvV6AQT4=";
        String feademo = "7qpXQpFlAACAAAAAM44RPf3/vbzRdM89hqCwvROqq73WBKM9Gv7DvbSaBz4T0Z87+d9mvFGoQD4VYcw9ohXAvc174L25er8970QePrsPBT5JRRc9XPL1PJAR0jvmZo68G1ArverBkL01FIq9OLuGveId5jxn77+8xkWUvcDVlz30tq29Vq5Qu3j1Q7svrCA+8NipvCyu6juNzk49T6r3PCzyez4Dri++TL1oPBsnI75Pl329koQNvkWSwT1qB1I9fAHDvYzfMT7jvtg9paBdPcLyDj7XxOA9kA+evQZYKL4n/KK83rKYvQEVkj2x7hO+pKKtvUV/gb1Pqro772bdPcSaAb1hqz09oBDEPQwnA72tGjU+W0JovdbSyr36PzE8B30xvUipET5MM1+9zFaRvaApnTw6uU29V3VuPUFljD3OH6I8ImMLvbQMaDyHDY69ePFOvcuzI70N7sa9aF6iuwlLwbkggMA9eaYJPZlgST2OqwG9NyR+vdDOIr2jij4+1c8nvesaoL24n0q82ZQ0PZF5EL4+zCK9JzB/vGzVBL5BcRg+WkbpPFMCjr1Lckc+SJdKvU3MPb5Ceqw9augRvYJhFD2LUE29xmwXPon8tD3yS4292SswPkiK6j3O08g9lNEEvMd1ET19rGi8vA3MPOUIwjwf1Iy9rsfrO0A4MDzVI1W7n+CYvQl4QLw=";

        String cxdboli = "7qpXQpFlAACAAAAAZb1WPQJZozzPVDa9tchuPpWVYT1+wKG9ObuJvTTOWb1DUX+8lLZlPAqjZjwSRFU9+p2SvX18NryOF5O9XqXOvVz/Hj6WTra9YmtaPh55Pz2GEYG9REz1PVx9Uz26ngA9hQoevipKtD0ONBG8gNkRPQyLsD2WxRW8aLJuvYc4BL3BD4I8zDCRvU1AZj0KPQ0+0rEkPSdlK70mRT29B5aXvUOgdD0gtNq8A+YKvgY4HL336Ts8Amzyvcm7jr3CYVQ9dPqBvfr1ljwAqr+9EKhNPZQ5HT6TJRE9egznPbOuBb5Vrv+9iue1vILaSL24CpI9GNapPP064r0/TJG8YD0Cvlx2Sb24ZZi9mv7Cvf8VPjw1oDe8XyVAPX6k/j3X8Bs+ySBgvHWuAL3sE+S7QAejvTQjfL3sBsk9RH0bPhGePTtjczS7MQUoPuLoFb4cVB2+trkfPZBZor1kgBc8LjnLPOuc4jygqX69kNi6PWvXjD2ttQw+GdECvn6gIb4xa409/MTlPJQP0TzxVME9jsuZvR/QLT29MqS9TOYuPc3AoL0hlPA76uEXPmkEEz0Xc9Q5jRsIPhd/YL5Eoxs+naU2Pi0q+r323b07nGk+vciT7T1bu/O9cLupPWanGD4OBzI9MxezvPjGsjtwDhy+Q//5vHasoDzGW1O9efrlvIO8v7w=";
        String cxd = "7qpXQpFlAACAAAAAqr7UvY5q4bruc/Y95BFoPQUcED6KRhs9wX4fvA697jnTX4I886XPPc0UYD3ycsm8PokFPqiVUT08RvK88pGOPGGJD7vsANA8f3fvu1LLIr46WaW82CAZPbGpGb7LxrM9M/d6PenT5r2mx+Q8IGaMu7vMR7xjKoG8PkoHvZhR9DwWscg999OmvTcvUj1e/JI96wPNvVZivbzjXWO9Y6MDPcQxuj1g+VG9CJ1vPezRDzxl18Y8+iYCvlIBzz0jaIG9Oo2Ru9kzjj7fgQU+7qaPPTB0Bj4McN08z7KKPH8bTjvT8LG95Oy6PHeIT75ERjK+agoavgrJfr0KEA2+yr+cPXdMXr0+idm9bbK5vIVwM740V1S9lo6xu7h9mD3F2Nu6Wryzvf/g6jxTyNK9bTcEveK46z0B6K+8YvOFvFjEyr0/gxA+TbeMvS1h9r3XHHO8WHtxvZiRQD2YTGu9CB08vs+Urb2mHhA9WkJcvUtcADw0q8M9ieLcvFtykz1UXQs9AhVuvXyP6TxScQk+9tjgPSwErT3ofFQ8NYttvHsTgT7y2MS8kM1Jvg7YIb51G6g9S4G6PYYiUj0I7CI9rwd/PSwY771sbrG8qGrzPW459j1tLVC9/ycZPfEV6D08Lby73yRqPO2iUzyLEEw92D6uPdW1ar37nBG9fj4wPvfXHr0=";
        float sim = 0.0f;

        Base64 base64 = new Base64();
        byte[] feature1 = base64.decode(fea1);
        byte[] feature2 = base64.decode(fea2);

        sim = fc.Comp(feature1, feature2);
        System.out.println("sim test = " + sim);

		/*
         * // compare base64 System.out.println("compare base64"); sim =
		 * fc.Comp(f1, f2); System.out.printf("sim=%f\n", sim);
		 * 
		 * long beginTime = 0; long endTime = 0;
		 * 
		 * int loop = 10000000; // compare binary java.util.Base64.Decoder
		 * base64decoder = java.util.Base64.getDecoder(); byte[] bin1 =
		 * base64decoder.decode(f1); byte[] bin2 = base64decoder.decode(f2);
		 * 
		 * System.out.println("compare binary"); beginTime =
		 * System.currentTimeMillis(); for (int i = 0; i < loop; i++) { sim =
		 * fc.Comp(bin1, bin2); } endTime = System.currentTimeMillis();
		 * System.out.printf("sim=%f, time=%dms\n", sim, endTime - beginTime);
		 * 
		 * // compare raw byte[] raw1 = base64decoder.decode(f1.substring(16));
		 * byte[] raw2 = base64decoder.decode(f2.substring(16));
		 * System.out.println("compare raw"); beginTime =
		 * System.currentTimeMillis(); for (int i = 0; i < loop; i++) { sim =
		 * fc.CompRawFeature(raw1, raw2); } endTime =
		 * System.currentTimeMillis(); System.out.printf("sim=%f, time=%dms\n",
		 * sim, endTime - beginTime);
		 */
    }


    /**
     * Compare two feature (support 253 model)
     *
     * @param f1 base64 encode feature
     * @param f2 base64 encode feature
     * @return similarity
     * @throws Exception
     */
    public float Comp(String f1, String f2) throws Exception {

        if (f1.length() != f2.length()) {
            throw new Exception("feature size unequal");
        }

        byte[] bin1 = base64decoder.decode(f1);
        byte[] bin2 = base64decoder.decode(f2);

        return Normalize(Dot(bin1, bin2, 12));
    }

    /**
     * Compare two binary feature with 12 bytes head (support 253 model)
     *
     * @param f1 binary feature with 12 bytes head
     * @param f2 binary feature with 12 bytes head
     * @return similarity
     * @throws Exception
     */
    public float Comp(byte[] f1, byte[] f2) throws Exception {
        int m1 = GetInt(f1, 0);
        int m2 = GetInt(f2, 0);
        int v1 = GetInt(f1, 4);
        int v2 = GetInt(f2, 4);
        int dim1 = GetInt(f1, 8);
        int dim2 = GetInt(f2, 8);

        if (v1 != v2) {
            throw new Exception("version unmatch");
        }

        if (0x4257aaee != m1) {
            dim1 = (dim1 - 12) / 4;
        }

        if (0x4257aaee != m2) {
            dim2 = (dim2 - 12) / 4;
        }

        if (dim1 != dim2) {
            throw new Exception("feature dimension unmatch");
        }

        // System.out.printf("m1=0x%x, m2=0x%x, v1=%d, v2=%d, dim1=%d,
        // dim2=%d\n", m1, m2, v1, v2, dim1, dim2);

        return Normalize(Dot(f1, f2, 12));
    }

    /**
     * Compare two raw feature with no head (support 253 model)
     *
     * @param f1 binary raw feature with no head
     * @param f2 binary raw feature with no head
     * @return similarity
     * @throws Exception
     */
    public float CompRawFeature(byte[] f1, byte[] f2) throws Exception {
        return Normalize(Dot(f1, f2, 0));
    }

    private float Dot(byte[] f1, byte[] f2, int offset) throws Exception {

        if (f1.length != f2.length) {
            throw new Exception("feature length unmatch");
        }

        if (0 != (f1.length - offset) % 4) {
            throw new Exception("feature dimension is incompeleted");
        }

        if (f1.length < offset) {
            throw new Exception("feature length is too short");
        }

        int dimCnt = (f1.length - offset) / 4;

        if (0 > dimCnt) {
            throw new Exception("");
        }

        float dist = 0.0f;
        for (int i = 0; i < dimCnt; i++) {
            dist += Float.intBitsToFloat(GetInt(f1, offset)) * Float.intBitsToFloat(GetInt(f2, offset));
            offset += 4;
        }

        return dist;
    }

    public int GetInt(byte[] bytes, int offset) {
        return (0xff & bytes[offset]) | (0xff00 & (bytes[offset + 1] << 8)) | (0xff0000 & (bytes[offset + 2] << 16))
                | (0xff000000 & (bytes[offset + 3] << 24));
    }

    private float Normalize(float score) {
        if (score <= src_points[0]) {
            return dst_points[0];
        } else if (score >= src_points[src_points.length - 1]) {
            return dst_points[dst_points.length - 1];
        }

        float result = 0.0f;

        for (int i = 1; i < src_points.length; i++) {
            if (score < src_points[i]) {
                result = dst_points[i - 1] + (score - src_points[i - 1]) * (dst_points[i] - dst_points[i - 1])
                        / (src_points[i] - src_points[i - 1]);
                break;
            }
        }

        return result;
    }

    private java.util.Base64.Decoder base64decoder = java.util.Base64.getDecoder();
    private float[] src_points = {0.0f, 0.128612995148f, 0.236073002219f, 0.316282004118f, 0.382878988981f,
            0.441266000271f, 0.490464001894f, 1.0f};
    private float[] dst_points = {0.0f, 0.40000000596f, 0.5f, 0.600000023842f, 0.699999988079f, 0.800000011921f,
            0.899999976158f, 1.0f};

}
